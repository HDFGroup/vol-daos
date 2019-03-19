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

#include "vol_attribute_test.h"

/*
 * XXX: Add test for creating a large attribute.
 */

static int test_create_attribute_on_root(void);
static int test_create_attribute_on_dataset(void);
static int test_create_attribute_on_datatype(void);
static int test_create_attribute_with_null_space(void);
static int test_create_attribute_with_scalar_space(void);
static int test_create_attribute_with_space_in_name(void);
static int test_create_attribute_invalid_params(void);
static int test_open_attribute(void);
static int test_open_attribute_invalid_params(void);
static int test_write_attribute(void);
static int test_write_attribute_invalid_params(void);
static int test_read_attribute(void);
static int test_read_attribute_invalid_params(void);
static int test_close_attribute_invalid_id(void);
static int test_get_attribute_space_and_type(void);
static int test_get_attribute_space_and_type_invalid_params(void);
static int test_attribute_property_lists(void);
static int test_get_attribute_name(void);
static int test_get_attribute_name_invalid_params(void);
static int test_get_attribute_storage_size(void);
static int test_get_attribute_info(void);
static int test_get_attribute_info_invalid_params(void);
static int test_rename_attribute(void);
static int test_rename_attribute_invalid_params(void);
static int test_attribute_iterate(void);
static int test_attribute_iterate_invalid_params(void);
static int test_attribute_iterate_0_attributes(void);
static int test_delete_attribute(void);
static int test_delete_attribute_invalid_params(void);
static int test_attribute_exists(void);
static int test_attribute_exists_invalid_params(void);
static int test_get_number_attributes(void);

static herr_t attr_iter_callback1(hid_t location_id, const char *attr_name, const H5A_info_t *ainfo, void *op_data);
static herr_t attr_iter_callback2(hid_t location_id, const char *attr_name, const H5A_info_t *ainfo, void *op_data);

/*
 * The array of attribute tests to be performed.
 */
static int (*attribute_tests[])(void) = {
        test_create_attribute_on_root,
        test_create_attribute_on_dataset,
        test_create_attribute_on_datatype,
        test_create_attribute_with_null_space,
        test_create_attribute_with_scalar_space,
        test_create_attribute_with_space_in_name,
        test_create_attribute_invalid_params,
        test_open_attribute,
        test_open_attribute_invalid_params,
        test_write_attribute,
        test_write_attribute_invalid_params,
        test_read_attribute,
        test_read_attribute_invalid_params,
        test_close_attribute_invalid_id,
        test_get_attribute_space_and_type,
        test_get_attribute_space_and_type_invalid_params,
        test_attribute_property_lists,
        test_get_attribute_name,
        test_get_attribute_name_invalid_params,
        test_get_attribute_storage_size,
        test_get_attribute_info,
        test_get_attribute_info_invalid_params,
        test_rename_attribute,
        test_rename_attribute_invalid_params,
        test_attribute_iterate,
        test_attribute_iterate_invalid_params,
        test_attribute_iterate_0_attributes,
        test_delete_attribute,
        test_delete_attribute_invalid_params,
        test_attribute_exists,
        test_attribute_exists_invalid_params,
        test_get_number_attributes,
};

/*
 * A test to check that an attribute can be created on
 * the root group.
 */
static int
test_create_attribute_on_root(void)
{
    hsize_t dims[ATTRIBUTE_CREATE_ON_ROOT_SPACE_RANK];
    size_t  i;
    htri_t  attr_exists;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   attr_id = H5I_INVALID_HID, attr_id2 = H5I_INVALID_HID;
    hid_t   attr_dtype1 = H5I_INVALID_HID, attr_dtype2 = H5I_INVALID_HID;
    hid_t   space_id = H5I_INVALID_HID;

    TESTING("attribute creation on the root group"); HDputs("");

    TESTING_2("H5Acreate on the root group")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    for (i = 0; i < ATTRIBUTE_CREATE_ON_ROOT_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((space_id = H5Screate_simple(ATTRIBUTE_CREATE_ON_ROOT_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_dtype1 = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR
    if ((attr_dtype2 = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((attr_id = H5Acreate2(file_id, ATTRIBUTE_CREATE_ON_ROOT_ATTR_NAME, attr_dtype1, space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute '%s' using H5Acreate\n", ATTRIBUTE_CREATE_ON_ROOT_ATTR_NAME);
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(file_id, ATTRIBUTE_CREATE_ON_ROOT_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute '%s' exists\n", ATTRIBUTE_CREATE_ON_ROOT_ATTR_NAME);
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute '%s' did not exist\n", ATTRIBUTE_CREATE_ON_ROOT_ATTR_NAME);
        goto error;
    }

    PASSED();

    TESTING_2("H5Acreate_by_name on the root group")

    if ((attr_id2 = H5Acreate_by_name(file_id, "/", ATTRIBUTE_CREATE_ON_ROOT_ATTR_NAME2, attr_dtype2, space_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute on root group using H5Acreate_by_name\n");
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(file_id, ATTRIBUTE_CREATE_ON_ROOT_ATTR_NAME2)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute '%s' exists\n", ATTRIBUTE_CREATE_ON_ROOT_ATTR_NAME2);
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute '%s' did not exist\n", ATTRIBUTE_CREATE_ON_ROOT_ATTR_NAME2);
        goto error;
    }

    if (H5Sclose(space_id) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype1) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype2) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id2) < 0)
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
        H5Tclose(attr_dtype1);
        H5Tclose(attr_dtype2);
        H5Aclose(attr_id);
        H5Aclose(attr_id2);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that an attribute can be created on
 * a dataset.
 */
static int
test_create_attribute_on_dataset(void)
{
    hsize_t dset_dims[ATTRIBUTE_CREATE_ON_DATASET_DSET_SPACE_RANK];
    hsize_t attr_dims[ATTRIBUTE_CREATE_ON_DATASET_ATTR_SPACE_RANK];
    size_t  i;
    htri_t  attr_exists;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID;
    hid_t   group_id = H5I_INVALID_HID;
    hid_t   dset_id = H5I_INVALID_HID;
    hid_t   attr_id = H5I_INVALID_HID, attr_id2 = H5I_INVALID_HID;
    hid_t   attr_dtype1 = H5I_INVALID_HID, attr_dtype2 = H5I_INVALID_HID;
    hid_t   dset_dtype = H5I_INVALID_HID;
    hid_t   dset_space_id = H5I_INVALID_HID;
    hid_t   attr_space_id = H5I_INVALID_HID;

    TESTING("attribute creation on a dataset"); HDputs("");

    TESTING_2("H5Acreate on a dataset")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", ATTRIBUTE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_CREATE_ON_DATASET_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", ATTRIBUTE_CREATE_ON_DATASET_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < ATTRIBUTE_CREATE_ON_DATASET_DSET_SPACE_RANK; i++)
        dset_dims[i] = (hsize_t) rand() % MAX_DIM_SIZE + 1;
    for (i = 0; i < ATTRIBUTE_CREATE_ON_DATASET_ATTR_SPACE_RANK; i++)
        attr_dims[i] = (hsize_t) rand() % MAX_DIM_SIZE + 1;

    if ((dset_space_id = H5Screate_simple(ATTRIBUTE_CREATE_ON_DATASET_DSET_SPACE_RANK, dset_dims, NULL)) < 0)
        TEST_ERROR
    if ((attr_space_id = H5Screate_simple(ATTRIBUTE_CREATE_ON_DATASET_ATTR_SPACE_RANK, attr_dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR
    if ((attr_dtype1 = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR
    if ((attr_dtype2 = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, ATTRIBUTE_CREATE_ON_DATASET_DSET_NAME, dset_dtype,
            dset_space_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset\n");
        goto error;
    }

    if ((attr_id = H5Acreate2(dset_id, ATTRIBUTE_CREATE_ON_DATASET_ATTR_NAME, attr_dtype1,
            attr_space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(dset_id, ATTRIBUTE_CREATE_ON_DATASET_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute '%s' exists\n", ATTRIBUTE_CREATE_ON_DATASET_ATTR_NAME);
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute '%s' did not exist\n", ATTRIBUTE_CREATE_ON_DATASET_ATTR_NAME);
        goto error;
    }

    PASSED();

    TESTING_2("H5Acreate_by_name on a dataset")

    if ((attr_id2 = H5Acreate_by_name(group_id, ATTRIBUTE_CREATE_ON_DATASET_DSET_NAME, ATTRIBUTE_CREATE_ON_DATASET_ATTR_NAME2,
            attr_dtype2, attr_space_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute on dataset by name\n");
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(dset_id, ATTRIBUTE_CREATE_ON_DATASET_ATTR_NAME2)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute '%s' exists\n", ATTRIBUTE_CREATE_ON_DATASET_ATTR_NAME2);
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute '%s' did not exist\n", ATTRIBUTE_CREATE_ON_DATASET_ATTR_NAME2);
        goto error;
    }

    if (H5Sclose(dset_space_id) < 0)
        TEST_ERROR
    if (H5Sclose(attr_space_id) < 0)
        TEST_ERROR
    if (H5Tclose(dset_dtype) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype1) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype2) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id2) < 0)
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
        H5Sclose(dset_space_id);
        H5Sclose(attr_space_id);
        H5Tclose(dset_dtype);
        H5Tclose(attr_dtype1);
        H5Tclose(attr_dtype2);
        H5Dclose(dset_id);
        H5Aclose(attr_id);
        H5Aclose(attr_id2);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that an attribute can be created on
 * a committed datatype.
 */
static int
test_create_attribute_on_datatype(void)
{
    hsize_t dims[ATTRIBUTE_CREATE_ON_DATATYPE_SPACE_RANK];
    size_t  i;
    htri_t  attr_exists;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID;
    hid_t   group_id = H5I_INVALID_HID;
    hid_t   type_id = H5I_INVALID_HID;
    hid_t   attr_id = H5I_INVALID_HID, attr_id2 = H5I_INVALID_HID;
    hid_t   attr_dtype1 = H5I_INVALID_HID, attr_dtype2 = H5I_INVALID_HID;
    hid_t   space_id = H5I_INVALID_HID;

    TESTING("attribute creation on a committed datatype"); HDputs("");

    TESTING_2("H5Acreate on a committed datatype")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", ATTRIBUTE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_CREATE_ON_DATATYPE_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", ATTRIBUTE_CREATE_ON_DATATYPE_GROUP_NAME);
    }

    if ((type_id = generate_random_datatype(H5T_NO_CLASS)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create datatype\n");
        goto error;
    }

    if (H5Tcommit2(group_id, ATTRIBUTE_CREATE_ON_DATATYPE_DTYPE_NAME, type_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't commit datatype\n");
        goto error;
    }

    {
        /* Temporary workaround for now since H5Tcommit2 doesn't return something public useable
         * for a VOL object */
        if (H5Tclose(type_id) < 0)
            TEST_ERROR

        if ((type_id = H5Topen2(group_id, ATTRIBUTE_CREATE_ON_DATATYPE_DTYPE_NAME, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDprintf("    couldn't open committed datatype\n");
            goto error;
        }
    }

    for (i = 0; i < ATTRIBUTE_CREATE_ON_DATATYPE_SPACE_RANK; i++)
        dims[i] = (hsize_t) rand() % MAX_DIM_SIZE + 1;

    if ((space_id = H5Screate_simple(ATTRIBUTE_CREATE_ON_DATATYPE_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_dtype1 = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR
    if ((attr_dtype2 = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((attr_id = H5Acreate2(type_id, ATTRIBUTE_CREATE_ON_DATATYPE_ATTR_NAME, attr_dtype1,
            space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute on datatype using H5Acreate\n");
        goto error;
    }

    if ((attr_exists = H5Aexists(type_id, ATTRIBUTE_CREATE_ON_DATATYPE_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Acreate_by_name on a committed datatype")

    if ((attr_id2 = H5Acreate_by_name(group_id, ATTRIBUTE_CREATE_ON_DATATYPE_DTYPE_NAME,
            ATTRIBUTE_CREATE_ON_DATATYPE_ATTR_NAME2, attr_dtype2, space_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute on datatype using H5Acreate_by_name\n");
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(type_id, ATTRIBUTE_CREATE_ON_DATATYPE_ATTR_NAME2)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    if (H5Sclose(space_id) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype1) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype2) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id2) < 0)
        TEST_ERROR
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
        H5Sclose(space_id);
        H5Tclose(attr_dtype1);
        H5Tclose(attr_dtype2);
        H5Aclose(attr_id);
        H5Aclose(attr_id2);
        H5Tclose(type_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that creating an attribute with a
 * NULL dataspace is not problematic.
 */
static int
test_create_attribute_with_null_space(void)
{
    htri_t attr_exists;
    hid_t  file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t  container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t  attr_id = H5I_INVALID_HID;
    hid_t  attr_dtype = H5I_INVALID_HID;
    hid_t  space_id = H5I_INVALID_HID;

    TESTING("attribute creation with a NULL dataspace")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group\n");
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_CREATE_NULL_DATASPACE_TEST_SUBGROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container subgroup\n");
        goto error;
    }

    if ((space_id = H5Screate(H5S_NULL)) < 0)
        TEST_ERROR

    if ((attr_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR


    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_CREATE_NULL_DATASPACE_TEST_ATTR_NAME, attr_dtype, space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_CREATE_NULL_DATASPACE_TEST_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    if (H5Aclose(attr_id) < 0)
        TEST_ERROR

    if ((attr_id = H5Aopen(group_id, ATTRIBUTE_CREATE_NULL_DATASPACE_TEST_ATTR_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open attribute\n");
        goto error;
    }

    if (H5Sclose(space_id) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype) < 0)
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
        H5Sclose(space_id);
        H5Tclose(attr_dtype);
        H5Aclose(attr_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that creating an attribute with a
 * scalar dataspace is not problematic.
 */
static int
test_create_attribute_with_scalar_space(void)
{
    htri_t attr_exists;
    hid_t  file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t  container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t  attr_id = H5I_INVALID_HID;
    hid_t  attr_dtype = H5I_INVALID_HID;
    hid_t  space_id = H5I_INVALID_HID;

    TESTING("attribute creation with a SCALAR dataspace")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group\n");
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_CREATE_SCALAR_DATASPACE_TEST_SUBGROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container subgroup\n");
        goto error;
    }

    if ((space_id = H5Screate(H5S_SCALAR)) < 0)
        TEST_ERROR

    if ((attr_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR


    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_CREATE_SCALAR_DATASPACE_TEST_ATTR_NAME, attr_dtype, space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_CREATE_SCALAR_DATASPACE_TEST_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    if (H5Aclose(attr_id) < 0)
        TEST_ERROR

    if ((attr_id = H5Aopen(group_id, ATTRIBUTE_CREATE_SCALAR_DATASPACE_TEST_ATTR_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open attribute\n");
        goto error;
    }

    if (H5Sclose(space_id) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype) < 0)
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
        H5Sclose(space_id);
        H5Tclose(attr_dtype);
        H5Aclose(attr_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that a space in an attribute's name
 * is not problematic.
 */
static int
test_create_attribute_with_space_in_name(void)
{
    hsize_t dims[ATTRIBUTE_CREATE_WITH_SPACE_IN_NAME_SPACE_RANK];
    size_t  i;
    htri_t  attr_exists;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID;
    hid_t   group_id = H5I_INVALID_HID;
    hid_t   attr_id = H5I_INVALID_HID;
    hid_t   attr_dtype = H5I_INVALID_HID;
    hid_t   space_id = H5I_INVALID_HID;

    TESTING("attribute creation with a space in attribute's name")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group\n");
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_CREATE_WITH_SPACE_IN_NAME_GROUP_NAME, H5P_DEFAULT,  H5P_DEFAULT,  H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", ATTRIBUTE_CREATE_WITH_SPACE_IN_NAME_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < ATTRIBUTE_CREATE_WITH_SPACE_IN_NAME_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((space_id = H5Screate_simple(ATTRIBUTE_CREATE_WITH_SPACE_IN_NAME_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_CREATE_WITH_SPACE_IN_NAME_ATTR_NAME, attr_dtype,
            space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_CREATE_WITH_SPACE_IN_NAME_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    if (H5Sclose(space_id) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype) < 0)
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
        H5Sclose(space_id);
        H5Tclose(attr_dtype);
        H5Aclose(attr_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that an attribute can't be created when
 * H5Acreate is passed invalid parameters.
 */
static int
test_create_attribute_invalid_params(void)
{
    hsize_t dims[ATTRIBUTE_CREATE_INVALID_PARAMS_SPACE_RANK];
    size_t  i;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t   attr_id = H5I_INVALID_HID;
    hid_t   attr_dtype = H5I_INVALID_HID;
    hid_t   space_id = H5I_INVALID_HID;

    TESTING("H5Acreate with invalid parameters"); HDputs("");

    TESTING_2("H5Acreate with invalid loc_id")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", ATTRIBUTE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_CREATE_INVALID_PARAMS_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group\n");
        goto error;
    }

    for (i = 0; i < ATTRIBUTE_CREATE_INVALID_PARAMS_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((space_id = H5Screate_simple(ATTRIBUTE_CREATE_INVALID_PARAMS_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR
    if ((attr_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    H5E_BEGIN_TRY {
        attr_id = H5Acreate2(H5I_INVALID_HID, ATTRIBUTE_CREATE_INVALID_PARAMS_ATTR_NAME, attr_dtype, space_id,
                H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    created attribute using H5Acreate with an invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Acreate with invalid attribute name")

    H5E_BEGIN_TRY {
        attr_id = H5Acreate2(container_group, NULL, attr_dtype, space_id, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    created attribute using H5Acreate with an invalid name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        attr_id = H5Acreate2(container_group, "", attr_dtype, space_id, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    created attribute using H5Acreate with an invalid name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Acreate with an invalid datatype")

    H5E_BEGIN_TRY {
        attr_id = H5Acreate2(container_group, ATTRIBUTE_CREATE_INVALID_PARAMS_ATTR_NAME, H5I_INVALID_HID,
                space_id, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    created attribute using H5Acreate with an invalid datatype!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Acreate with an invalid dataspace")

    H5E_BEGIN_TRY {
        attr_id = H5Acreate2(container_group, ATTRIBUTE_CREATE_INVALID_PARAMS_ATTR_NAME, attr_dtype,
                H5I_INVALID_HID, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    created attribute using H5Acreate with an invalid dataspace!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Acreate with an invalid ACPL")

    H5E_BEGIN_TRY {
        attr_id = H5Acreate2(container_group, ATTRIBUTE_CREATE_INVALID_PARAMS_ATTR_NAME, attr_dtype,
                space_id, H5I_INVALID_HID, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    created attribute using H5Acreate with an invalid ACPL!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Acreate with an invalid AAPL")

    H5E_BEGIN_TRY {
        attr_id = H5Acreate2(container_group, ATTRIBUTE_CREATE_INVALID_PARAMS_ATTR_NAME, attr_dtype,
                space_id, H5P_DEFAULT, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    created attribute using H5Acreate with an invalid AAPL!\n");
        goto error;
    }

    PASSED();

    TESTING("H5Acreate_by_name with invalid parameters"); HDputs("");

    TESTING_2("H5Acreate_by_name with an invalid loc_id")

    H5E_BEGIN_TRY {
        attr_id = H5Acreate_by_name(H5I_INVALID_HID, ATTRIBUTE_CREATE_INVALID_PARAMS_GROUP_NAME,
                ATTRIBUTE_CREATE_INVALID_PARAMS_ATTR_NAME, attr_dtype, space_id, H5P_DEFAULT, H5P_DEFAULT,
                H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    created attribute using H5Acreate_by_name with an invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Acreate_by_name with invalid object name")

    H5E_BEGIN_TRY {
        attr_id = H5Acreate_by_name(file_id, NULL, ATTRIBUTE_CREATE_INVALID_PARAMS_ATTR_NAME, attr_dtype,
                space_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    created attribute using H5Acreate_by_name with an invalid object name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        attr_id = H5Acreate_by_name(file_id, "", ATTRIBUTE_CREATE_INVALID_PARAMS_ATTR_NAME, attr_dtype,
                space_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    created attribute using H5Acreate_by_name with an invalid object name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Acreate_by_name with invalid attribute name")

    H5E_BEGIN_TRY {
        attr_id = H5Acreate_by_name(file_id, ATTRIBUTE_CREATE_INVALID_PARAMS_GROUP_NAME,
                NULL, attr_dtype, space_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    created attribute using H5Acreate_by_name with an invalid attribute name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        attr_id = H5Acreate_by_name(file_id, ATTRIBUTE_CREATE_INVALID_PARAMS_GROUP_NAME,
                "", attr_dtype, space_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    created attribute using H5Acreate_by_name with an invalid attribute name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Acreate_by_name with invalid datatype")

    H5E_BEGIN_TRY {
        attr_id = H5Acreate_by_name(file_id, ATTRIBUTE_CREATE_INVALID_PARAMS_GROUP_NAME,
                ATTRIBUTE_CREATE_INVALID_PARAMS_ATTR_NAME, H5I_INVALID_HID, space_id, H5P_DEFAULT,
                H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    created attribute using H5Acreate_by_name with an invalid datatype!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Acreate_by_name with invalid dataspace")

    H5E_BEGIN_TRY {
        attr_id = H5Acreate_by_name(file_id, ATTRIBUTE_CREATE_INVALID_PARAMS_GROUP_NAME,
                ATTRIBUTE_CREATE_INVALID_PARAMS_ATTR_NAME, attr_dtype, H5I_INVALID_HID, H5P_DEFAULT,
                H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    created attribute using H5Acreate_by_name with an invalid dataspace!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Acreate_by_name with invalid ACPL")

    H5E_BEGIN_TRY {
        attr_id = H5Acreate_by_name(file_id, ATTRIBUTE_CREATE_INVALID_PARAMS_GROUP_NAME,
                ATTRIBUTE_CREATE_INVALID_PARAMS_ATTR_NAME, attr_dtype, space_id, H5I_INVALID_HID,
                H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    created attribute using H5Acreate_by_name with an invalid ACPL!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Acreate_by_name with invalid AAPL")

    H5E_BEGIN_TRY {
        attr_id = H5Acreate_by_name(file_id, ATTRIBUTE_CREATE_INVALID_PARAMS_GROUP_NAME,
                ATTRIBUTE_CREATE_INVALID_PARAMS_ATTR_NAME, attr_dtype, space_id, H5P_DEFAULT,
                H5I_INVALID_HID, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    created attribute using H5Acreate_by_name with an invalid AAPL!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Acreate_by_name with invalid LAPL")

    H5E_BEGIN_TRY {
        attr_id = H5Acreate_by_name(file_id, ATTRIBUTE_CREATE_INVALID_PARAMS_GROUP_NAME,
                ATTRIBUTE_CREATE_INVALID_PARAMS_ATTR_NAME, attr_dtype, space_id, H5P_DEFAULT,
                H5P_DEFAULT, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    created attribute using H5Acreate_by_name with an invalid LAPL!\n");
        goto error;
    }

    if (H5Sclose(space_id) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype) < 0)
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
        H5Sclose(space_id);
        H5Tclose(attr_dtype);
        H5Aclose(attr_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test for H5Aopen.
 */
static int
test_open_attribute(void)
{
    hsize_t attr_dims[ATTRIBUTE_OPEN_TEST_SPACE_RANK];
    size_t  i;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t   attr_id = H5I_INVALID_HID;
    hid_t   space_id = H5I_INVALID_HID;
    hid_t   attr_type = H5I_INVALID_HID;

    TESTING("attribute opening"); HDputs("");

    TESTING_2("H5Aopen")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", ATTRIBUTE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_OPEN_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", ATTRIBUTE_OPEN_TEST_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < ATTRIBUTE_OPEN_TEST_SPACE_RANK; i++)
        attr_dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((space_id = H5Screate_simple(ATTRIBUTE_OPEN_TEST_SPACE_RANK, attr_dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_type = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_OPEN_TEST_ATTR_NAME, attr_type, space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute '%s'\n", ATTRIBUTE_OPEN_TEST_ATTR_NAME);
        goto error;
    }

    if (H5Aclose(attr_id) < 0)
        TEST_ERROR

    if ((attr_id = H5Aopen(group_id, ATTRIBUTE_OPEN_TEST_ATTR_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open attribute '%s' using H5Aopen\n", ATTRIBUTE_OPEN_TEST_ATTR_NAME);
        goto error;
    }

    /*
     * XXX: Check something.
     */

    if (H5Aclose(attr_id) < 0)
        TEST_ERROR

    PASSED();

    TESTING_2("H5Aopen_by_name")

    if ((attr_id = H5Aopen_by_name(container_group, ATTRIBUTE_OPEN_TEST_GROUP_NAME,
            ATTRIBUTE_OPEN_TEST_ATTR_NAME, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open attribute '%s' using H5Aopen_by_name\n", ATTRIBUTE_OPEN_TEST_ATTR_NAME);
        goto error;
    }

    if (H5Aclose(attr_id) < 0)
        TEST_ERROR

    PASSED();

    TESTING_2("H5Aopen_by_idx")

    if ((attr_id = H5Aopen_by_idx(container_group, ATTRIBUTE_OPEN_TEST_GROUP_NAME, H5_INDEX_NAME,
            H5_ITER_INC, 0, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open attribute '%s' using H5Aopen_by_idx\n", ATTRIBUTE_OPEN_TEST_ATTR_NAME);
        goto error;
    }

    if (H5Sclose(space_id) < 0)
        TEST_ERROR
    if (H5Tclose(attr_type) < 0)
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
        H5Sclose(space_id);
        H5Tclose(attr_type);
        H5Aclose(attr_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that an attribute can't be opened when
 * H5Aopen(_by_name/_by_idx) is passed invalid parameters.
 */
static int
test_open_attribute_invalid_params(void)
{
    hsize_t attr_dims[ATTRIBUTE_OPEN_TEST_SPACE_RANK];
    size_t  i;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t   attr_id = H5I_INVALID_HID;
    hid_t   space_id = H5I_INVALID_HID;
    hid_t   attr_type = H5I_INVALID_HID;

    TESTING("H5Aopen with invalid parameters"); HDputs("");

    TESTING_2("H5Aopen with an invalid loc_id")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", ATTRIBUTE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_SPACE_RANK; i++)
        attr_dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((space_id = H5Screate_simple(ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_SPACE_RANK, attr_dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_type = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR


    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME, attr_type, space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute '%s'\n", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME);
        goto error;
    }

    if (H5Aclose(attr_id) < 0)
        TEST_ERROR

    H5E_BEGIN_TRY {
        attr_id = H5Aopen(H5I_INVALID_HID, ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    opened attribute '%s' using H5Aopen with an invalid loc_id!\n", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME);
        goto error;
    }

    PASSED();

    TESTING_2("H5Aopen with an invalid attribute name")

    H5E_BEGIN_TRY {
        attr_id = H5Aopen(group_id, NULL, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    opened attribute '%s' using H5Aopen with an invalid attribute name!\n", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME);
        goto error;
    }

    H5E_BEGIN_TRY {
        attr_id = H5Aopen(group_id, "", H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    opened attribute '%s' using H5Aopen with an invalid attribute name!\n", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME);
        goto error;
    }

    PASSED();

    TESTING_2("H5Aopen with an invalid AAPL")

    H5E_BEGIN_TRY {
        attr_id = H5Aopen(group_id, ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    opened attribute '%s' using H5Aopen with an invalid AAPL!\n", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME);
        goto error;
    }

    PASSED();

    TESTING_2("H5Aopen_by_name with an invalid loc_id")

    H5E_BEGIN_TRY {
        attr_id = H5Aopen_by_name(H5I_INVALID_HID, ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_GROUP_NAME,
                ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    opened attribute '%s' using H5Aopen_by_name with an invalid loc_id!\n", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME);
        goto error;
    }

    PASSED();

    TESTING("H5Aopen_by_name with invalid parameters"); HDputs("");

    TESTING_2("H5Aopen_by_name with an invalid object name")

    H5E_BEGIN_TRY {
        attr_id = H5Aopen_by_name(container_group, NULL, ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    opened attribute '%s' using H5Aopen_by_name with an invalid object name!\n", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME);
        goto error;
    }

    H5E_BEGIN_TRY {
        attr_id = H5Aopen_by_name(container_group, "", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    opened attribute '%s' using H5Aopen_by_name with an invalid object name!\n", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME);
        goto error;
    }

    PASSED();

    TESTING_2("H5Aopen_by_name with an invalid attribute name")

    H5E_BEGIN_TRY {
        attr_id = H5Aopen_by_name(container_group, ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_GROUP_NAME, NULL, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    opened attribute '%s' using H5Aopen_by_name with an invalid attribute name!\n", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME);
        goto error;
    }

    H5E_BEGIN_TRY {
        attr_id = H5Aopen_by_name(container_group, ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_GROUP_NAME, "", H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    opened attribute '%s' using H5Aopen_by_name with an invalid attribute name!\n", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME);
        goto error;
    }

    PASSED();

    TESTING_2("H5Aopen_by_name with an invalid AAPL")

    H5E_BEGIN_TRY {
        attr_id = H5Aopen_by_name(container_group, ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_GROUP_NAME,
                ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME, H5I_INVALID_HID, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    opened attribute '%s' using H5Aopen_by_name with an invalid AAPL!\n", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME);
        goto error;
    }

    PASSED();

    TESTING_2("H5Aopen_by_name with an invalid LAPL")

    H5E_BEGIN_TRY {
        attr_id = H5Aopen_by_name(container_group, ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_GROUP_NAME,
                ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME, H5P_DEFAULT, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    opened attribute '%s' using H5Aopen_by_name with an invalid LAPL!\n", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME);
        goto error;
    }

    PASSED();

    TESTING("H5Aopen_by_idx with invalid parameters"); HDputs("");

    TESTING_2("H5Aopen_by_idx with an invalid loc_id")

    H5E_BEGIN_TRY {
        attr_id = H5Aopen_by_idx(H5I_INVALID_HID, ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_GROUP_NAME, H5_INDEX_NAME,
                H5_ITER_INC, 0, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    opened attribute '%s' using H5Aopen_by_idx with an invalid loc_id!\n", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME);
        goto error;
    }

    PASSED();

    TESTING_2("H5Aopen_by_idx with an invalid object name")

    H5E_BEGIN_TRY {
        attr_id = H5Aopen_by_idx(container_group, NULL, H5_INDEX_NAME, H5_ITER_INC, 0, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    opened attribute '%s' using H5Aopen_by_idx with an invalid object name!\n", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME);
        goto error;
    }

    H5E_BEGIN_TRY {
        attr_id = H5Aopen_by_idx(container_group, "", H5_INDEX_NAME, H5_ITER_INC, 0, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    opened attribute '%s' using H5Aopen_by_idx with an invalid object name!\n", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME);
        goto error;
    }

    PASSED();

    TESTING_2("H5Aopen_by_idx with an invalid index type")

    H5E_BEGIN_TRY {
        attr_id = H5Aopen_by_idx(container_group, ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_GROUP_NAME, H5_INDEX_UNKNOWN,
                H5_ITER_INC, 0, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    opened attribute '%s' using H5Aopen_by_idx with an invalid index type!\n", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME);
        goto error;
    }

    H5E_BEGIN_TRY {
        attr_id = H5Aopen_by_idx(container_group, ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_GROUP_NAME, H5_INDEX_N,
                H5_ITER_INC, 0, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    opened attribute '%s' using H5Aopen_by_idx with an invalid index type!\n", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME);
        goto error;
    }

    PASSED();

    TESTING_2("H5Aopen_by_idx with an invalid iteration order")

    H5E_BEGIN_TRY {
        attr_id = H5Aopen_by_idx(container_group, ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_GROUP_NAME, H5_INDEX_NAME,
                H5_ITER_UNKNOWN, 0, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    opened attribute '%s' using H5Aopen_by_idx with an invalid iteration order!\n", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME);
        goto error;
    }

    H5E_BEGIN_TRY {
        attr_id = H5Aopen_by_idx(container_group, ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_GROUP_NAME, H5_INDEX_NAME,
                H5_ITER_N, 0, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    opened attribute '%s' using H5Aopen_by_idx with an invalid iteration order!\n", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME);
        goto error;
    }

    PASSED();

    TESTING_2("H5Aopen_by_idx with an invalid AAPL")

    H5E_BEGIN_TRY {
        attr_id = H5Aopen_by_idx(container_group, ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_GROUP_NAME, H5_INDEX_NAME,
                H5_ITER_INC, 0, H5I_INVALID_HID, H5P_DEFAULT);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    opened attribute '%s' using H5Aopen_by_idx with an invalid AAPL!\n", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME);
        goto error;
    }

    PASSED();

    TESTING_2("H5Aopen_by_idx with an invalid LAPL")

    H5E_BEGIN_TRY {
        attr_id = H5Aopen_by_idx(container_group, ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_GROUP_NAME, H5_INDEX_NAME,
                H5_ITER_INC, 0, H5P_DEFAULT, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (attr_id >= 0) {
        H5_FAILED();
        HDprintf("    opened attribute '%s' using H5Aopen_by_idx with an invalid LAPL!\n", ATTRIBUTE_OPEN_INVALID_PARAMS_TEST_ATTR_NAME);
        goto error;
    }

    if (H5Sclose(space_id) < 0)
        TEST_ERROR
    if (H5Tclose(attr_type) < 0)
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
        H5Sclose(space_id);
        H5Tclose(attr_type);
        H5Aclose(attr_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that a simple write to an attribute
 * can be made.
 */
static int
test_write_attribute(void)
{
    hsize_t  dims[ATTRIBUTE_WRITE_TEST_SPACE_RANK];
    size_t   i, data_size;
    htri_t   attr_exists;
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    container_group = H5I_INVALID_HID;
    hid_t    group_id = H5I_INVALID_HID;
    hid_t    attr_id = H5I_INVALID_HID;
    hid_t    space_id = H5I_INVALID_HID;
    void    *data = NULL;

    TESTING("H5Awrite")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group\n");
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_WRITE_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", ATTRIBUTE_WRITE_TEST_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < ATTRIBUTE_WRITE_TEST_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((space_id = H5Screate_simple(ATTRIBUTE_WRITE_TEST_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_WRITE_TEST_ATTR_NAME, ATTRIBUTE_WRITE_TEST_ATTR_DTYPE,
            space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_WRITE_TEST_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    for (i = 0, data_size = 1; i < ATTRIBUTE_WRITE_TEST_SPACE_RANK; i++)
        data_size *= dims[i];
    data_size *= ATTRIBUTE_WRITE_TEST_ATTR_DTYPE_SIZE;

    if (NULL == (data = HDmalloc(data_size)))
        TEST_ERROR

    for (i = 0; i < data_size / ATTRIBUTE_WRITE_TEST_ATTR_DTYPE_SIZE; i++)
        ((int *) data)[i] = (int) i;

    if (H5Awrite(attr_id, ATTRIBUTE_WRITE_TEST_ATTR_DTYPE, data) < 0) {
        H5_FAILED();
        HDprintf("    couldn't write to attribute\n");
        goto error;
    }

    if (data) {
        HDfree(data);
        data = NULL;
    }

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
        if (data) HDfree(data);
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
 * A test to check that writing an attribute fails when
 * H5Awrite is passed invalid parameters.
 */
static int
test_write_attribute_invalid_params(void)
{
    hsize_t  dims[ATTRIBUTE_WRITE_INVALID_PARAMS_TEST_SPACE_RANK];
    size_t   i, data_size;
    htri_t   attr_exists;
    herr_t   err_ret = -1;
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    container_group = H5I_INVALID_HID;
    hid_t    group_id = H5I_INVALID_HID;
    hid_t    attr_id = H5I_INVALID_HID;
    hid_t    space_id = H5I_INVALID_HID;
    void    *data = NULL;

    TESTING("H5Awrite with invalid parameters"); HDputs("");

    TESTING_2("H5Awrite with an invalid attr_id")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", ATTRIBUTE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_WRITE_INVALID_PARAMS_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", ATTRIBUTE_WRITE_INVALID_PARAMS_TEST_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < ATTRIBUTE_WRITE_INVALID_PARAMS_TEST_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((space_id = H5Screate_simple(ATTRIBUTE_WRITE_INVALID_PARAMS_TEST_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_WRITE_INVALID_PARAMS_TEST_ATTR_NAME, ATTRIBUTE_WRITE_INVALID_PARAMS_TEST_ATTR_DTYPE,
            space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_WRITE_INVALID_PARAMS_TEST_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    for (i = 0, data_size = 1; i < ATTRIBUTE_WRITE_INVALID_PARAMS_TEST_SPACE_RANK; i++)
        data_size *= dims[i];
    data_size *= ATTRIBUTE_WRITE_INVALID_PARAMS_TEST_ATTR_DTYPE_SIZE;

    if (NULL == (data = HDmalloc(data_size)))
        TEST_ERROR

    for (i = 0; i < data_size / ATTRIBUTE_WRITE_INVALID_PARAMS_TEST_ATTR_DTYPE_SIZE; i++)
        ((int *) data)[i] = (int) i;

    H5E_BEGIN_TRY {
        err_ret = H5Awrite(H5I_INVALID_HID, ATTRIBUTE_WRITE_INVALID_PARAMS_TEST_ATTR_DTYPE, data);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    wrote to attribute using an invalid attr_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Awrite with an invalid datatype")

    H5E_BEGIN_TRY {
        err_ret = H5Awrite(attr_id, H5I_INVALID_HID, data);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    wrote to attribute using an invalid datatype!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Awrite with an invalid data buffer")

    H5E_BEGIN_TRY {
        err_ret = H5Awrite(attr_id, ATTRIBUTE_WRITE_INVALID_PARAMS_TEST_ATTR_DTYPE, NULL);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    wrote to attribute using an invalid data buffer!\n");
        goto error;
    }

    if (data) {
        HDfree(data);
        data = NULL;
    }

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
        if (data) HDfree(data);
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
 * A test to check that simple data can be read back
 * and verified after it has been written to an
 * attribute.
 */
static int
test_read_attribute(void)
{
    hsize_t  dims[ATTRIBUTE_READ_TEST_SPACE_RANK];
    size_t   i, data_size;
    htri_t   attr_exists;
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    container_group = H5I_INVALID_HID;
    hid_t    group_id = H5I_INVALID_HID;
    hid_t    attr_id = H5I_INVALID_HID;
    hid_t    space_id = H5I_INVALID_HID;
    void    *data = NULL;
    void    *read_buf = NULL;

    TESTING("H5Aread")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", ATTRIBUTE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_READ_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    could't create container group '%s'\n", ATTRIBUTE_READ_TEST_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < ATTRIBUTE_READ_TEST_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((space_id = H5Screate_simple(ATTRIBUTE_READ_TEST_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_READ_TEST_ATTR_NAME, ATTRIBUTE_READ_TEST_ATTR_DTYPE,
            space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_READ_TEST_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    for (i = 0, data_size = 1; i < ATTRIBUTE_READ_TEST_SPACE_RANK; i++)
        data_size *= dims[i];
    data_size *= ATTRIBUTE_READ_TEST_ATTR_DTYPE_SIZE;

    if (NULL == (data = HDmalloc(data_size)))
        TEST_ERROR
    if (NULL == (read_buf = HDcalloc(1, data_size)))
        TEST_ERROR

    for (i = 0; i < data_size / ATTRIBUTE_READ_TEST_ATTR_DTYPE_SIZE; i++)
        ((int *) data)[i] = (int) i;

    if (H5Awrite(attr_id, ATTRIBUTE_READ_TEST_ATTR_DTYPE, data) < 0) {
        H5_FAILED();
        HDprintf("    couldn't write to attribute\n");
        goto error;
    }

    if (data) {
        HDfree(data);
        data = NULL;
    }

    if (H5Aclose(attr_id) < 0)
        TEST_ERROR

    if ((attr_id = H5Aopen(group_id, ATTRIBUTE_READ_TEST_ATTR_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open attribute\n");
        goto error;
    }

    if (H5Aread(attr_id, ATTRIBUTE_READ_TEST_ATTR_DTYPE, read_buf) < 0) {
        H5_FAILED();
        HDprintf("    couldn't read from attribute\n");
        goto error;
    }

    for (i = 0; i < data_size / ATTRIBUTE_READ_TEST_ATTR_DTYPE_SIZE; i++) {
        if (((int *) read_buf)[i] != (int) i) {
            H5_FAILED();
            HDprintf("    data verification failed\n");
            goto error;
        }
    }

    if (read_buf) {
        HDfree(read_buf);
        read_buf = NULL;
    }

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
        if (data) HDfree(data);
        if (read_buf) HDfree(read_buf);
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
 * A test to check that reading an attribute fails when
 * H5Aread is passed invalid parameters.
 */
static int
test_read_attribute_invalid_params(void)
{
    hsize_t  dims[ATTRIBUTE_READ_INVALID_PARAMS_TEST_SPACE_RANK];
    size_t   i, data_size;
    htri_t   attr_exists;
    herr_t   err_ret = -1;
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    container_group = H5I_INVALID_HID;
    hid_t    group_id = H5I_INVALID_HID;
    hid_t    attr_id = H5I_INVALID_HID;
    hid_t    space_id = H5I_INVALID_HID;
    void    *data = NULL;
    void    *read_buf = NULL;

    TESTING("H5Aread with invalid parameters"); HDputs("");

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", ATTRIBUTE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_READ_INVALID_PARAMS_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    could't create container group '%s'\n", ATTRIBUTE_READ_INVALID_PARAMS_TEST_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < ATTRIBUTE_READ_INVALID_PARAMS_TEST_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((space_id = H5Screate_simple(ATTRIBUTE_READ_INVALID_PARAMS_TEST_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_READ_INVALID_PARAMS_TEST_ATTR_NAME, ATTRIBUTE_READ_INVALID_PARAMS_TEST_ATTR_DTYPE,
            space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_READ_INVALID_PARAMS_TEST_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    for (i = 0, data_size = 1; i < ATTRIBUTE_READ_INVALID_PARAMS_TEST_SPACE_RANK; i++)
        data_size *= dims[i];
    data_size *= ATTRIBUTE_READ_INVALID_PARAMS_TEST_ATTR_DTYPE_SIZE;

    if (NULL == (data = HDmalloc(data_size)))
        TEST_ERROR
    if (NULL == (read_buf = HDcalloc(1, data_size)))
        TEST_ERROR

    for (i = 0; i < data_size / ATTRIBUTE_READ_INVALID_PARAMS_TEST_ATTR_DTYPE_SIZE; i++)
        ((int *) data)[i] = (int) i;

    if (H5Awrite(attr_id, ATTRIBUTE_READ_INVALID_PARAMS_TEST_ATTR_DTYPE, data) < 0) {
        H5_FAILED();
        HDprintf("    couldn't write to attribute\n");
        goto error;
    }

    if (data) {
        HDfree(data);
        data = NULL;
    }

    if (H5Aclose(attr_id) < 0)
        TEST_ERROR

    if ((attr_id = H5Aopen(group_id, ATTRIBUTE_READ_INVALID_PARAMS_TEST_ATTR_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open attribute\n");
        goto error;
    }

    TESTING_2("H5Aread with an invalid attr_id")

    H5E_BEGIN_TRY {
        err_ret = H5Aread(H5I_INVALID_HID, ATTRIBUTE_READ_INVALID_PARAMS_TEST_ATTR_DTYPE, read_buf);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    read attribute with an invalid attr_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aread with an invalid datatype")

    H5E_BEGIN_TRY {
        err_ret = H5Aread(attr_id, H5I_INVALID_HID, read_buf);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    read attribute with an invalid datatype!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aread with an invalid read buffer")

    H5E_BEGIN_TRY {
        err_ret = H5Aread(attr_id, ATTRIBUTE_READ_INVALID_PARAMS_TEST_ATTR_DTYPE, NULL);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    read attribute with an invalid read buffer!\n");
        goto error;
    }

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
        if (data) HDfree(data);
        if (read_buf) HDfree(read_buf);
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
 * A test to check that H5Aclose fails when it is passed
 * an invalid attribute ID.
 */
static int
test_close_attribute_invalid_id(void)
{
    herr_t err_ret = -1;
    hid_t  file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;

    TESTING("H5Aclose with an invalid attribute ID")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Aclose(H5I_INVALID_HID);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    H5Aclose succeeded with an invalid attribute ID!\n");
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
 * A test to check that valid copies of an attribute's
 * dataspace and datatype can be retrieved with
 * H5Aget_space and H5Aget_type, respectively.
 */
static int
test_get_attribute_space_and_type(void)
{
    hsize_t attr_dims[ATTRIBUTE_GET_SPACE_TYPE_TEST_SPACE_RANK];
    size_t  i;
    htri_t  attr_exists;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID;
    hid_t   group_id = H5I_INVALID_HID;
    hid_t   attr_id = H5I_INVALID_HID;
    hid_t   attr_dtype = H5I_INVALID_HID;
    hid_t   attr_space_id = H5I_INVALID_HID;
    hid_t   tmp_type_id = H5I_INVALID_HID;
    hid_t   tmp_space_id = H5I_INVALID_HID;

    TESTING("retrieval of an attribute's dataspace and datatype"); HDputs("");

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", ATTRIBUTE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_GET_SPACE_TYPE_TEST_GROUP_NAME, H5P_DEFAULT,  H5P_DEFAULT,  H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", ATTRIBUTE_GET_SPACE_TYPE_TEST_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < ATTRIBUTE_GET_SPACE_TYPE_TEST_SPACE_RANK; i++)
        attr_dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((attr_space_id = H5Screate_simple(ATTRIBUTE_GET_SPACE_TYPE_TEST_SPACE_RANK, attr_dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_GET_SPACE_TYPE_TEST_ATTR_NAME, attr_dtype,
            attr_space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_GET_SPACE_TYPE_TEST_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    /* Retrieve the attribute's datatype and dataspace and verify them */
    TESTING_2("H5Aget_type")

    if ((tmp_type_id = H5Aget_type(attr_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve attribute's datatype\n");
        goto error;
    }

    {
        htri_t types_equal = H5Tequal(tmp_type_id, attr_dtype);

        if (types_equal < 0) {
            H5_FAILED();
            HDprintf("    datatype was invalid\n");
            goto error;
        }

        if (!types_equal) {
            H5_FAILED();
            HDprintf("    attribute's datatype did not match\n");
            goto error;
        }
    }

    PASSED();

    TESTING_2("H5Aget_space")

    if ((tmp_space_id = H5Aget_space(attr_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve attribute's dataspace\n");
        goto error;
    }

    {
        hsize_t space_dims[ATTRIBUTE_GET_SPACE_TYPE_TEST_SPACE_RANK];

        if (H5Sget_simple_extent_dims(tmp_space_id, space_dims, NULL) < 0)
            TEST_ERROR

        for (i = 0; i < ATTRIBUTE_GET_SPACE_TYPE_TEST_SPACE_RANK; i++)
            if (space_dims[i] != attr_dims[i]) {
                H5_FAILED();
                HDprintf("    attribute's dataspace dims didn't match\n");
                goto error;
            }
    }

    PASSED();

    TESTING_2("H5Aget_type after re-opening an attribute")

    /* Now close the attribute and verify that this still works after opening an
     * attribute instead of creating it
     */
    if (H5Aclose(attr_id) < 0)
        TEST_ERROR
    if (H5Tclose(tmp_type_id) < 0)
        TEST_ERROR
    if (H5Sclose(tmp_space_id) < 0)
        TEST_ERROR

    if ((attr_id = H5Aopen(group_id, ATTRIBUTE_GET_SPACE_TYPE_TEST_ATTR_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open attribute\n");
        goto error;
    }

    if ((tmp_type_id = H5Aget_type(attr_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve attribute's datatype\n");
        goto error;
    }

    {
        htri_t types_equal = H5Tequal(tmp_type_id, attr_dtype);

        if (types_equal < 0) {
            H5_FAILED();
            HDprintf("    datatype was invalid\n");
            goto error;
        }

        if (!types_equal) {
            H5_FAILED();
            HDprintf("    attribute's datatype did not match\n");
            goto error;
        }
    }

    PASSED();

    TESTING_2("H5Aget_space after re-opening an attribute")

    if ((tmp_space_id = H5Aget_space(attr_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve attribute's dataspace\n");
        goto error;
    }

    {
        hsize_t space_dims[ATTRIBUTE_GET_SPACE_TYPE_TEST_SPACE_RANK];

        if (H5Sget_simple_extent_dims(tmp_space_id, space_dims, NULL) < 0)
            TEST_ERROR

        for (i = 0; i < ATTRIBUTE_GET_SPACE_TYPE_TEST_SPACE_RANK; i++) {
            if (space_dims[i] != attr_dims[i]) {
                H5_FAILED();
                HDprintf("    dataspace dims didn't match!\n");
                goto error;
            }
        }
    }

    if (H5Sclose(tmp_space_id) < 0)
        TEST_ERROR
    if (H5Sclose(attr_space_id) < 0)
        TEST_ERROR
    if (H5Tclose(tmp_type_id) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype) < 0)
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
        H5Sclose(tmp_space_id);
        H5Sclose(attr_space_id);
        H5Tclose(tmp_type_id);
        H5Tclose(attr_dtype);
        H5Aclose(attr_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that an attribute's dataspace and datatype
 * can't be retrieved when H5Aget_space and H5Aget_type are passed
 * invalid parameters, respectively.
 */
static int
test_get_attribute_space_and_type_invalid_params(void)
{
    hsize_t attr_dims[ATTRIBUTE_GET_SPACE_TYPE_TEST_SPACE_RANK];
    size_t  i;
    htri_t  attr_exists;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID;
    hid_t   group_id = H5I_INVALID_HID;
    hid_t   attr_id = H5I_INVALID_HID;
    hid_t   attr_dtype = H5I_INVALID_HID;
    hid_t   attr_space_id = H5I_INVALID_HID;
    hid_t   tmp_type_id = H5I_INVALID_HID;
    hid_t   tmp_space_id = H5I_INVALID_HID;

    TESTING("H5Aget_type/H5Aget_space with invalid parameters"); HDputs("");

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", ATTRIBUTE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_GET_SPACE_TYPE_INVALID_PARAMS_TEST_GROUP_NAME, H5P_DEFAULT,  H5P_DEFAULT,  H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", ATTRIBUTE_GET_SPACE_TYPE_INVALID_PARAMS_TEST_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < ATTRIBUTE_GET_SPACE_TYPE_INVALID_PARAMS_TEST_SPACE_RANK; i++)
        attr_dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((attr_space_id = H5Screate_simple(ATTRIBUTE_GET_SPACE_TYPE_INVALID_PARAMS_TEST_SPACE_RANK, attr_dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_GET_SPACE_TYPE_INVALID_PARAMS_TEST_ATTR_NAME, attr_dtype,
            attr_space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_GET_SPACE_TYPE_INVALID_PARAMS_TEST_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    /* Retrieve the attribute's datatype and dataspace and verify them */
    TESTING_2("H5Aget_type with an invalid attr_id")

    H5E_BEGIN_TRY {
        tmp_type_id = H5Aget_type(H5I_INVALID_HID);
    } H5E_END_TRY;

    if (tmp_type_id >= 0) {
        H5_FAILED();
        HDprintf("    retrieved copy of attribute's datatype using an invalid attr_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_space with an invalid attr_id")

    H5E_BEGIN_TRY {
        tmp_space_id = H5Aget_space(H5I_INVALID_HID);
    } H5E_END_TRY;

    if (tmp_space_id >= 0) {
        H5_FAILED();
        HDprintf("    retrieved copy of attribute's dataspace using an invalid attr_id!\n");
        goto error;
    }

    if (H5Sclose(attr_space_id) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype) < 0)
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
        H5Sclose(tmp_space_id);
        H5Sclose(attr_space_id);
        H5Tclose(tmp_type_id);
        H5Tclose(attr_dtype);
        H5Aclose(attr_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that a VOL connector stores and can
 * retrieve a valid copy of an ACPL used at attribute
 * creation time.
 */
static int
test_attribute_property_lists(void)
{
    H5T_cset_t encoding = H5T_CSET_UTF8;
    hsize_t    dims[ATTRIBUTE_PROPERTY_LIST_TEST_SPACE_RANK];
    size_t     i;
    htri_t     attr_exists;
    hid_t      file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t      container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t      attr_id1 = H5I_INVALID_HID, attr_id2 = H5I_INVALID_HID;
    hid_t      attr_dtype1 = H5I_INVALID_HID, attr_dtype2 = H5I_INVALID_HID;
    hid_t      acpl_id1 = H5I_INVALID_HID, acpl_id2 = H5I_INVALID_HID;
    hid_t      space_id = H5I_INVALID_HID;

    TESTING("attribute property list operations"); HDputs("");

    TESTING_2("H5Aget_create_plist")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group\n");
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_PROPERTY_LIST_TEST_SUBGROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group\n");
        goto error;
    }

    for (i = 0; i < ATTRIBUTE_PROPERTY_LIST_TEST_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((space_id = H5Screate_simple(ATTRIBUTE_PROPERTY_LIST_TEST_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_dtype1 = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR
    if ((attr_dtype2 = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((acpl_id1 = H5Pcreate(H5P_ATTRIBUTE_CREATE)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create ACPL\n");
        goto error;
    }

    if (H5Pset_char_encoding(acpl_id1, encoding) < 0) {
        H5_FAILED();
        HDprintf("    couldn't set ACPL property value\n");
        goto error;
    }

    if ((attr_id1 = H5Acreate2(group_id, ATTRIBUTE_PROPERTY_LIST_TEST_ATTRIBUTE_NAME1, attr_dtype1,
            space_id, acpl_id1, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    if ((attr_id2 = H5Acreate2(group_id, ATTRIBUTE_PROPERTY_LIST_TEST_ATTRIBUTE_NAME2, attr_dtype2,
            space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    if (H5Pclose(acpl_id1) < 0)
        TEST_ERROR

    /* Verify the attributes have been created */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_PROPERTY_LIST_TEST_ATTRIBUTE_NAME1)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_PROPERTY_LIST_TEST_ATTRIBUTE_NAME2)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    /* Try to retrieve copies of the two property lists, one which ahs the property set and one which does not */
    if ((acpl_id1 = H5Aget_create_plist(attr_id1)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get property list\n");
        goto error;
    }

    if ((acpl_id2 = H5Aget_create_plist(attr_id2)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get property list\n");
        goto error;
    }

    /* Ensure that property list 1 has the property list set and property list 2 does not */
    encoding = H5T_CSET_ERROR;

    if (H5Pget_char_encoding(acpl_id1, &encoding) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve ACPL property value\n");
        goto error;
    }

    if (H5T_CSET_UTF8 != encoding) {
        H5_FAILED();
        HDprintf("   ACPL property value was incorrect\n");
        goto error;
    }

    encoding = H5T_CSET_ERROR;

    if (H5Pget_char_encoding(acpl_id2, &encoding) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve ACPL property value\n");
        goto error;
    }

    if (H5T_CSET_UTF8 == encoding) {
        H5_FAILED();
        HDprintf("    ACPL property value was set!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_create_plist after re-opening an attribute")

    /* Now close the property lists and attribute and see if we can still retrieve copies of
     * the property lists upon opening (instead of creating) an attribute
     */
    if (H5Pclose(acpl_id1) < 0)
        TEST_ERROR
    if (H5Pclose(acpl_id2) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id1) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id2) < 0)
        TEST_ERROR

    if ((attr_id1 = H5Aopen(group_id, ATTRIBUTE_PROPERTY_LIST_TEST_ATTRIBUTE_NAME1, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open attribute\n");
        goto error;
    }

    if ((attr_id2 = H5Aopen(group_id, ATTRIBUTE_PROPERTY_LIST_TEST_ATTRIBUTE_NAME2, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open attribute\n");
        goto error;
    }

    if ((acpl_id1 = H5Aget_create_plist(attr_id1)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get property list\n");
        goto error;
    }

    if ((acpl_id2 = H5Aget_create_plist(attr_id2)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get property list\n");
        goto error;
    }

    if (H5Pclose(acpl_id1) < 0)
        TEST_ERROR
    if (H5Pclose(acpl_id2) < 0)
        TEST_ERROR
    if (H5Sclose(space_id) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype1) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype2) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id1) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id2) < 0)
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
        H5Pclose(acpl_id1);
        H5Pclose(acpl_id2);
        H5Sclose(space_id);
        H5Tclose(attr_dtype1);
        H5Tclose(attr_dtype2);
        H5Aclose(attr_id1);
        H5Aclose(attr_id2);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that an attribute's name can be
 * correctly retrieved with H5Aget_name and
 * H5Aget_name_by_idx.
 */
static int
test_get_attribute_name(void)
{
    hsize_t  dims[ATTRIBUTE_GET_NAME_TEST_SPACE_RANK];
    ssize_t  name_buf_size;
    size_t   i;
    htri_t   attr_exists;
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    container_group = H5I_INVALID_HID;
    hid_t    group_id = H5I_INVALID_HID;
    hid_t    attr_id = H5I_INVALID_HID;
    hid_t    attr_dtype = H5I_INVALID_HID;
    hid_t    space_id = H5I_INVALID_HID;
    char    *name_buf = NULL;

    TESTING("retrieval of an attribute's name"); HDputs("");

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", ATTRIBUTE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_GET_NAME_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", ATTRIBUTE_GET_NAME_TEST_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < ATTRIBUTE_GET_NAME_TEST_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((space_id = H5Screate_simple(ATTRIBUTE_GET_NAME_TEST_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_GET_NAME_TEST_ATTRIBUTE_NAME, attr_dtype,
            space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_GET_NAME_TEST_ATTRIBUTE_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    TESTING_2("H5Aget_name")

    /* Retrieve the name buffer size */
    if ((name_buf_size = H5Aget_name(attr_id, 0, NULL)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve name buf size\n");
        goto error;
    }

    if (NULL == (name_buf = (char *) HDmalloc((size_t) name_buf_size + 1)))
        TEST_ERROR

    if (H5Aget_name(attr_id, (size_t) name_buf_size + 1, name_buf) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve attribute name\n");
    }

    if (HDstrcmp(name_buf, ATTRIBUTE_GET_NAME_TEST_ATTRIBUTE_NAME)) {
        H5_FAILED();
        HDprintf("    retrieved attribute name '%s' didn't match '%s'\n", name_buf, ATTRIBUTE_GET_NAME_TEST_ATTRIBUTE_NAME);
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_name_by_idx")

    if (H5Aget_name_by_idx(container_group, ATTRIBUTE_GET_NAME_TEST_GROUP_NAME, H5_INDEX_NAME, H5_ITER_INC,
            0, name_buf, (size_t) name_buf_size + 1, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve attribute name by index\n");
        goto error;
    }

    if (HDstrcmp(name_buf, ATTRIBUTE_GET_NAME_TEST_ATTRIBUTE_NAME)) {
        H5_FAILED();
        HDprintf("    attribute name didn't match\n");
        goto error;
    }

    PASSED();

    /* Now close the attribute and verify that we can still retrieve the attribute's name after
     * opening (instead of creating) it
     */
    if (H5Aclose(attr_id) < 0)
        TEST_ERROR

    if ((attr_id = H5Aopen(group_id, ATTRIBUTE_GET_NAME_TEST_ATTRIBUTE_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open attribute\n");
        goto error;
    }

    TESTING_2("H5Aget_name after re-opening an attribute")

    if (H5Aget_name(attr_id, (size_t) name_buf_size + 1, name_buf) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve attribute name\n");
        goto error;
    }

    if (HDstrcmp(name_buf, ATTRIBUTE_GET_NAME_TEST_ATTRIBUTE_NAME)) {
        H5_FAILED();
        HDprintf("    attribute name didn't match\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_name_by_idx after re-opening an attribute")

    if (H5Aget_name_by_idx(container_group, ATTRIBUTE_GET_NAME_TEST_GROUP_NAME, H5_INDEX_NAME, H5_ITER_INC,
            0, name_buf, (size_t) name_buf_size + 1, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve attribute name by index\n");
        goto error;
    }

    if (HDstrcmp(name_buf, ATTRIBUTE_GET_NAME_TEST_ATTRIBUTE_NAME)) {
        H5_FAILED();
        HDprintf("    attribute name didn't match\n");
        goto error;
    }

    if (name_buf) {
        HDfree(name_buf);
        name_buf = NULL;
    }

    if (H5Sclose(space_id) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype) < 0)
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
        if (name_buf) HDfree(name_buf);
        H5Sclose(space_id);
        H5Tclose(attr_dtype);
        H5Aclose(attr_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that an attribute's name can't be
 * retrieved when H5Aget_name(_by_idx) is passed invalid
 * parameters.
 */
static int
test_get_attribute_name_invalid_params(void)
{
    hsize_t  dims[ATTRIBUTE_GET_NAME_TEST_SPACE_RANK];
    ssize_t  name_buf_size;
    size_t   i;
    htri_t   attr_exists;
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    container_group = H5I_INVALID_HID;
    hid_t    group_id = H5I_INVALID_HID;
    hid_t    attr_id = H5I_INVALID_HID;
    hid_t    attr_dtype = H5I_INVALID_HID;
    hid_t    space_id = H5I_INVALID_HID;
    char    *name_buf = NULL;

    TESTING("retrieval of an attribute's name with invalid parameters"); HDputs("");

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", ATTRIBUTE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_GET_NAME_INVALID_PARAMS_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", ATTRIBUTE_GET_NAME_INVALID_PARAMS_TEST_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < ATTRIBUTE_GET_NAME_INVALID_PARAMS_TEST_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((space_id = H5Screate_simple(ATTRIBUTE_GET_NAME_INVALID_PARAMS_TEST_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_GET_NAME_INVALID_PARAMS_TEST_ATTRIBUTE_NAME, attr_dtype,
            space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_GET_NAME_INVALID_PARAMS_TEST_ATTRIBUTE_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    /*
     * Allocate an actual buffer for the tests.
     */

    if ((name_buf_size = H5Aget_name(attr_id, 0, NULL)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve name buf size\n");
        goto error;
    }

    if (NULL == (name_buf = (char *) HDmalloc((size_t) name_buf_size + 1)))
        TEST_ERROR

    TESTING_2("H5Aget_name with an invalid attr_id")

    H5E_BEGIN_TRY {
        name_buf_size = H5Aget_name(H5I_INVALID_HID, (size_t) name_buf_size + 1, name_buf);
    } H5E_END_TRY;

    if (name_buf_size >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute name using H5Aget_name with an invalid attr_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_name with an invalid name buffer")

    H5E_BEGIN_TRY {
        name_buf_size = 1;
        name_buf_size = H5Aget_name(attr_id, (size_t) name_buf_size, NULL);
    } H5E_END_TRY;

    if (name_buf_size >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute name using H5Aget_name with an invalid name buffer!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_name_by_idx with an invalid loc_id")

    H5E_BEGIN_TRY {
        name_buf_size = H5Aget_name_by_idx(H5I_INVALID_HID, ATTRIBUTE_GET_NAME_INVALID_PARAMS_TEST_GROUP_NAME,
                H5_INDEX_NAME, H5_ITER_INC, 0, name_buf, (size_t) name_buf_size + 1, H5P_DEFAULT);
    } H5E_END_TRY;

    if (name_buf_size >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute name using H5Aget_name_by_idx with an invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_name_by_idx with an invalid object name")

    H5E_BEGIN_TRY {
        name_buf_size = H5Aget_name_by_idx(container_group, NULL, H5_INDEX_NAME, H5_ITER_INC,
                0, name_buf, (size_t) name_buf_size + 1, H5P_DEFAULT);
    } H5E_END_TRY;

    if (name_buf_size >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute name using H5Aget_name_by_idx with an invalid object name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        name_buf_size = H5Aget_name_by_idx(container_group, "", H5_INDEX_NAME, H5_ITER_INC,
                0, name_buf, (size_t) name_buf_size + 1, H5P_DEFAULT);
    } H5E_END_TRY;

    if (name_buf_size >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute name using H5Aget_name_by_idx with an invalid object name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_name_by_idx with an invalid index type")

    H5E_BEGIN_TRY {
        name_buf_size = H5Aget_name_by_idx(container_group, ATTRIBUTE_GET_NAME_INVALID_PARAMS_TEST_GROUP_NAME,
                H5_INDEX_UNKNOWN, H5_ITER_INC, 0, name_buf, (size_t) name_buf_size + 1, H5P_DEFAULT);
    } H5E_END_TRY;

    if (name_buf_size >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute name using H5Aget_name_by_idx with an invalid index type!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        name_buf_size = H5Aget_name_by_idx(container_group, ATTRIBUTE_GET_NAME_INVALID_PARAMS_TEST_GROUP_NAME,
                H5_INDEX_N, H5_ITER_INC, 0, name_buf, (size_t) name_buf_size + 1, H5P_DEFAULT);
    } H5E_END_TRY;

    if (name_buf_size >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute name using H5Aget_name_by_idx with an invalid index type!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_name_by_idx with an invalid iteration order")

    H5E_BEGIN_TRY {
        name_buf_size = H5Aget_name_by_idx(container_group, ATTRIBUTE_GET_NAME_INVALID_PARAMS_TEST_GROUP_NAME,
                H5_INDEX_NAME, H5_ITER_UNKNOWN, 0, name_buf, (size_t) name_buf_size + 1, H5P_DEFAULT);
    } H5E_END_TRY;

    if (name_buf_size >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute name using H5Aget_name_by_idx with an invalid iteration order!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        name_buf_size = H5Aget_name_by_idx(container_group, ATTRIBUTE_GET_NAME_INVALID_PARAMS_TEST_GROUP_NAME,
                H5_INDEX_NAME, H5_ITER_N, 0, name_buf, (size_t) name_buf_size + 1, H5P_DEFAULT);
    } H5E_END_TRY;

    if (name_buf_size >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute name using H5Aget_name_by_idx with an invalid iteration order!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_name_by_idx with an invalid name buffer")

    H5E_BEGIN_TRY {
        name_buf_size = 1;
        name_buf_size = H5Aget_name_by_idx(container_group, ATTRIBUTE_GET_NAME_INVALID_PARAMS_TEST_GROUP_NAME,
                H5_INDEX_NAME, H5_ITER_INC, 0, NULL, (size_t) name_buf_size, H5P_DEFAULT);
    } H5E_END_TRY;

    if (name_buf_size >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute name using H5Aget_name_by_idx with an invalid name buffer!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_name_by_idx with an invalid LAPL")

    H5E_BEGIN_TRY {
        name_buf_size = H5Aget_name_by_idx(container_group, ATTRIBUTE_GET_NAME_INVALID_PARAMS_TEST_GROUP_NAME,
                H5_INDEX_NAME, H5_ITER_INC, 0, name_buf, (size_t) name_buf_size + 1, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (name_buf_size >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute name using H5Aget_name_by_idx with an invalid LAPL!\n");
        goto error;
    }

    if (name_buf) {
        HDfree(name_buf);
        name_buf = NULL;
    }

    if (H5Sclose(space_id) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype) < 0)
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
        if (name_buf) HDfree(name_buf);
        H5Sclose(space_id);
        H5Tclose(attr_dtype);
        H5Aclose(attr_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test for H5Aget_storage_size.
 */
static int
test_get_attribute_storage_size(void)
{
    TESTING("H5Aget_storage_size");

    SKIPPED();

    return 0;
}

/*
 * A test to check the functionality of H5Aget_info.
 */
static int
test_get_attribute_info(void)
{
    H5A_info_t attr_info;
    hsize_t    dims[ATTRIBUTE_GET_INFO_TEST_SPACE_RANK];
    size_t     i;
    htri_t     attr_exists;
    hid_t      file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t      container_group = H5I_INVALID_HID;
    hid_t      group_id = H5I_INVALID_HID;
    hid_t      attr_id = H5I_INVALID_HID;
    hid_t      attr_dtype = H5I_INVALID_HID;
    hid_t      space_id = H5I_INVALID_HID;

    TESTING("retrieval of attribute info"); HDputs("");

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group\n");
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_GET_INFO_TEST_GROUP_NAME, H5P_DEFAULT,  H5P_DEFAULT,  H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", ATTRIBUTE_GET_INFO_TEST_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < ATTRIBUTE_GET_INFO_TEST_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((space_id = H5Screate_simple(ATTRIBUTE_GET_INFO_TEST_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_GET_INFO_TEST_ATTR_NAME, attr_dtype,
            space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_GET_INFO_TEST_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    TESTING_2("H5Aget_info")

    if (H5Aget_info(attr_id, &attr_info) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get attribute info\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_info_by_name")

    if (H5Aget_info_by_name(group_id, ".", ATTRIBUTE_GET_INFO_TEST_ATTR_NAME, &attr_info, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get attribute info by name\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_info_by_idx")

    if (H5Aget_info_by_idx(group_id, ".", H5_INDEX_NAME, H5_ITER_INC, 0, &attr_info, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get attribute info by index\n");
        goto error;
    }

    if (H5Sclose(space_id) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype) < 0)
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
        H5Sclose(space_id);
        H5Tclose(attr_dtype);
        H5Aclose(attr_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that H5Aget_info(_by_name/_by_idx)
 * doesn't succeed when passed invalid parameters.
 */
static int
test_get_attribute_info_invalid_params(void)
{
    H5A_info_t attr_info;
    hsize_t    dims[ATTRIBUTE_GET_INFO_TEST_SPACE_RANK];
    size_t     i;
    htri_t     attr_exists;
    herr_t     err_ret = -1;
    hid_t      file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t      container_group = H5I_INVALID_HID;
    hid_t      group_id = H5I_INVALID_HID;
    hid_t      attr_id = H5I_INVALID_HID;
    hid_t      attr_dtype = H5I_INVALID_HID;
    hid_t      space_id = H5I_INVALID_HID;

    TESTING("retrieval of attribute info with invalid parameters"); HDputs("");

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", ATTRIBUTE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_GET_INFO_INVALID_PARAMS_TEST_GROUP_NAME, H5P_DEFAULT,  H5P_DEFAULT,  H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", ATTRIBUTE_GET_INFO_INVALID_PARAMS_TEST_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < ATTRIBUTE_GET_INFO_INVALID_PARAMS_TEST_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((space_id = H5Screate_simple(ATTRIBUTE_GET_INFO_INVALID_PARAMS_TEST_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_GET_INFO_INVALID_PARAMS_TEST_ATTR_NAME, attr_dtype,
            space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_GET_INFO_INVALID_PARAMS_TEST_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    TESTING_2("H5Aget_info with an invalid attr_id")

    H5E_BEGIN_TRY {
        err_ret = H5Aget_info(H5I_INVALID_HID, &attr_info);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute info using H5Aget_info with an invalid attr_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_info with an invalid attribute info pointer")

    H5E_BEGIN_TRY {
        err_ret = H5Aget_info(attr_id, NULL);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute info using H5Aget_info with an invalid attr_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_info_by_name with an invalid loc_id")

    H5E_BEGIN_TRY {
        err_ret = H5Aget_info_by_name(H5I_INVALID_HID, ".", ATTRIBUTE_GET_INFO_INVALID_PARAMS_TEST_ATTR_NAME,
                &attr_info, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute info using H5Aget_info_by_name with an invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_info_by_name with an invalid object name")

    H5E_BEGIN_TRY {
        err_ret = H5Aget_info_by_name(group_id, NULL, ATTRIBUTE_GET_INFO_INVALID_PARAMS_TEST_ATTR_NAME,
                &attr_info, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute info using H5Aget_info_by_name with an invalid object name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Aget_info_by_name(group_id, "", ATTRIBUTE_GET_INFO_INVALID_PARAMS_TEST_ATTR_NAME,
                &attr_info, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute info using H5Aget_info_by_name with an invalid object name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_info_by_name with an invalid attribute name")

    H5E_BEGIN_TRY {
        err_ret = H5Aget_info_by_name(group_id, ".", NULL, &attr_info, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute info using H5Aget_info_by_name with an invalid attribute name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Aget_info_by_name(group_id, ".", "", &attr_info, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute info using H5Aget_info_by_name with an invalid attribute name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_info_by_name with an invalid attribute info pointer")

    H5E_BEGIN_TRY {
        err_ret = H5Aget_info_by_name(group_id, ".", ATTRIBUTE_GET_INFO_INVALID_PARAMS_TEST_ATTR_NAME,
                NULL, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute info using H5Aget_info_by_name with an invalid attribute info pointer!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_info_by_name with an invalid LAPL")

    H5E_BEGIN_TRY {
        err_ret = H5Aget_info_by_name(group_id, ".", ATTRIBUTE_GET_INFO_INVALID_PARAMS_TEST_ATTR_NAME,
                &attr_info, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute info using H5Aget_info_by_name with an invalid LAPL!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_info_by_idx with an invalid loc_id")

    H5E_BEGIN_TRY {
        err_ret = H5Aget_info_by_idx(H5I_INVALID_HID, ".", H5_INDEX_NAME, H5_ITER_INC, 0, &attr_info, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute info using H5Aget_info_by_idx with an invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_info_by_idx with an invalid object name")

    H5E_BEGIN_TRY {
        err_ret = H5Aget_info_by_idx(group_id, NULL, H5_INDEX_NAME, H5_ITER_INC, 0, &attr_info, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute info using H5Aget_info_by_idx with an invalid object name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Aget_info_by_idx(group_id, "", H5_INDEX_NAME, H5_ITER_INC, 0, &attr_info, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute info using H5Aget_info_by_idx with an invalid object name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_info_by_idx with an invalid index type")

    H5E_BEGIN_TRY {
        err_ret = H5Aget_info_by_idx(group_id, ".", H5_INDEX_UNKNOWN, H5_ITER_INC, 0, &attr_info, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute info using H5Aget_info_by_idx with an invalid index type!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Aget_info_by_idx(group_id, ".", H5_INDEX_N, H5_ITER_INC, 0, &attr_info, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute info using H5Aget_info_by_idx with an invalid index type!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_info_by_idx with an invalid iteration order")

    H5E_BEGIN_TRY {
        err_ret = H5Aget_info_by_idx(group_id, ".", H5_INDEX_NAME, H5_ITER_UNKNOWN, 0, &attr_info, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute info using H5Aget_info_by_idx with an invalid iteration order!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Aget_info_by_idx(group_id, ".", H5_INDEX_NAME, H5_ITER_N, 0, &attr_info, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute info using H5Aget_info_by_idx with an invalid iteration order!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_info_by_idx with an invalid attribute info pointer")

    H5E_BEGIN_TRY {
        err_ret = H5Aget_info_by_idx(group_id, ".", H5_INDEX_NAME, H5_ITER_INC, 0, NULL, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute info using H5Aget_info_by_idx with an invalid attribute info pointer!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aget_info_by_idx with an invalid LAPL")

    H5E_BEGIN_TRY {
        err_ret = H5Aget_info_by_idx(group_id, ".", H5_INDEX_NAME, H5_ITER_INC, 0, &attr_info, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    retrieved attribute info using H5Aget_info_by_idx with an invalid LAPL!\n");
        goto error;
    }

    if (H5Sclose(space_id) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype) < 0)
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
        H5Sclose(space_id);
        H5Tclose(attr_dtype);
        H5Aclose(attr_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that an attribute can be renamed
 * with H5Arename and H5Arename_by_name.
 */
static int
test_rename_attribute(void)
{
    hsize_t attr_dims[ATTRIBUTE_RENAME_TEST_SPACE_RANK];
    size_t  i;
    htri_t  attr_exists;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID;
    hid_t   group_id = H5I_INVALID_HID;
    hid_t   attr_id = H5I_INVALID_HID, attr_id2 = H5I_INVALID_HID;
    hid_t   attr_dtype = H5I_INVALID_HID;
    hid_t   attr_space_id = H5I_INVALID_HID;

    TESTING("attribute renaming"); HDputs("");

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", ATTRIBUTE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_RENAME_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", ATTRIBUTE_RENAME_TEST_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < ATTRIBUTE_RENAME_TEST_SPACE_RANK; i++)
        attr_dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((attr_space_id = H5Screate_simple(ATTRIBUTE_RENAME_TEST_SPACE_RANK, attr_dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_RENAME_TEST_ATTR_NAME, attr_dtype,
            attr_space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    if ((attr_id2 = H5Acreate2(group_id, ATTRIBUTE_RENAME_TEST_ATTR_NAME2, attr_dtype,
            attr_space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attributes have been created */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_RENAME_TEST_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_RENAME_TEST_ATTR_NAME2)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    TESTING_2("H5Arename")

    if (H5Arename(group_id, ATTRIBUTE_RENAME_TEST_ATTR_NAME, ATTRIBUTE_RENAME_TEST_NEW_NAME) < 0) {
        H5_FAILED();
        HDprintf("    couldn't rename attribute '%s' to '%s' using H5Arename\n", ATTRIBUTE_RENAME_TEST_ATTR_NAME, ATTRIBUTE_RENAME_TEST_NEW_NAME);
        goto error;
    }

    PASSED();

    TESTING_2("H5Arename_by_name")

    if (H5Arename_by_name(container_group, ATTRIBUTE_RENAME_TEST_GROUP_NAME, ATTRIBUTE_RENAME_TEST_ATTR_NAME2,
            ATTRIBUTE_RENAME_TEST_NEW_NAME2, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't rename attribute '%s' to '%s' using H5Arename_by_name\n", ATTRIBUTE_RENAME_TEST_ATTR_NAME2, ATTRIBUTE_RENAME_TEST_NEW_NAME2);
        goto error;
    }

    /*
     * XXX: Check name if H5Aget_name is available and test renaming back to original name.
     */

    if (H5Sclose(attr_space_id) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id2) < 0)
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
        H5Sclose(attr_space_id);
        H5Tclose(attr_dtype);
        H5Aclose(attr_id);
        H5Aclose(attr_id2);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that an attribute can't be renamed
 * when H5Arename(_by_name) is passed invalid parameters.
 */
static int
test_rename_attribute_invalid_params(void)
{
    hsize_t attr_dims[ATTRIBUTE_RENAME_TEST_SPACE_RANK];
    size_t  i;
    htri_t  attr_exists;
    herr_t  err_ret = -1;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID;
    hid_t   group_id = H5I_INVALID_HID;
    hid_t   attr_id = H5I_INVALID_HID, attr_id2 = H5I_INVALID_HID;
    hid_t   attr_dtype = H5I_INVALID_HID;
    hid_t   attr_space_id = H5I_INVALID_HID;

    TESTING("attribute renaming with invalid parameters"); HDputs("");

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", ATTRIBUTE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_SPACE_RANK; i++)
        attr_dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((attr_space_id = H5Screate_simple(ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_SPACE_RANK, attr_dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_ATTR_NAME, attr_dtype,
            attr_space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    if ((attr_id2 = H5Acreate2(group_id, ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_ATTR_NAME2, attr_dtype,
            attr_space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attributes have been created */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_ATTR_NAME2)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    TESTING_2("H5Arename with an invalid loc_id")

    H5E_BEGIN_TRY {
        err_ret = H5Arename(H5I_INVALID_HID, ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_ATTR_NAME, ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_NEW_NAME);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    renamed attribute using H5Arename with an invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Arename with an invalid old attribute name")

    H5E_BEGIN_TRY {
        err_ret = H5Arename(group_id, NULL, ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_NEW_NAME);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    renamed attribute using H5Arename with an invalid old attribute name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Arename(group_id, "", ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_NEW_NAME);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    renamed attribute using H5Arename with an invalid old attribute name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Arename with an invalid new attribute name")

    H5E_BEGIN_TRY {
        err_ret = H5Arename(group_id, ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_ATTR_NAME, NULL);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    renamed attribute using H5Arename with an invalid new attribute name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Arename(group_id, ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_ATTR_NAME, "");
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    renamed attribute using H5Arename with an invalid new attribute name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Arename_by_name with an invalid loc_id")

    H5E_BEGIN_TRY {
        err_ret = H5Arename_by_name(H5I_INVALID_HID, ".", ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_ATTR_NAME,
                ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_NEW_NAME, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    renamed attribute using H5Arename_by_name with an invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Arename_by_name with an invalid object name")

    H5E_BEGIN_TRY {
        err_ret = H5Arename_by_name(group_id, NULL, ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_ATTR_NAME,
                ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_NEW_NAME, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    renamed attribute using H5Arename_by_name with an invalid object name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Arename_by_name(group_id, "", ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_ATTR_NAME,
                ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_NEW_NAME, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    renamed attribute using H5Arename_by_name with an invalid object name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Arename_by_name with an invalid old attribute name")

    H5E_BEGIN_TRY {
        err_ret = H5Arename_by_name(group_id, ".", NULL, ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_NEW_NAME, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    renamed attribute using H5Arename_by_name with an invalid old attribute name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Arename_by_name(group_id, ".", "", ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_NEW_NAME, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    renamed attribute using H5Arename_by_name with an invalid old attribute name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Arename_by_name with an invalid new attribute name")

    H5E_BEGIN_TRY {
        err_ret = H5Arename_by_name(group_id, ".", ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_ATTR_NAME, NULL, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    renamed attribute using H5Arename_by_name with an invalid new attribute name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Arename_by_name(group_id, ".", ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_ATTR_NAME, "", H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    renamed attribute using H5Arename_by_name with an invalid new attribute name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Arename_by_name with an invalid LAPL")

    H5E_BEGIN_TRY {
        err_ret = H5Arename_by_name(group_id, ".", ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_ATTR_NAME,
                ATTRIBUTE_RENAME_INVALID_PARAMS_TEST_NEW_NAME, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    renamed attribute using H5Arename_by_name with an invalid LAPL!\n");
        goto error;
    }

    if (H5Sclose(attr_space_id) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id2) < 0)
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
        H5Sclose(attr_space_id);
        H5Tclose(attr_dtype);
        H5Aclose(attr_id);
        H5Aclose(attr_id2);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check the functionality of attribute
 * iteration using H5Aiterate. Iteration is done
 * in increasing and decreasing order of both
 * attribute name and attribute creation order.
 */
static int
test_attribute_iterate(void)
{
    hsize_t dset_dims[ATTRIBUTE_ITERATE_TEST_DSET_SPACE_RANK];
    hsize_t attr_dims[ATTRIBUTE_ITERATE_TEST_ATTR_SPACE_RANK];
    size_t  i;
    htri_t  attr_exists;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t   dset_id = H5I_INVALID_HID;
    hid_t   attr_id = H5I_INVALID_HID, attr_id2 = H5I_INVALID_HID, attr_id3 = H5I_INVALID_HID, attr_id4 = H5I_INVALID_HID;
    hid_t   dset_dtype = H5I_INVALID_HID;
    hid_t   attr_dtype = H5I_INVALID_HID;
    hid_t   dset_space_id = H5I_INVALID_HID;
    hid_t   attr_space_id = H5I_INVALID_HID;

    TESTING("attribute iteration"); HDputs("");

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group\n");
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_ITERATE_TEST_SUBGROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container subgroup\n");
        goto error;
    }

    if ((dset_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR
    if ((attr_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    for (i = 0; i < ATTRIBUTE_ITERATE_TEST_DSET_SPACE_RANK; i++)
        dset_dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);
    for (i = 0; i < ATTRIBUTE_ITERATE_TEST_ATTR_SPACE_RANK; i++)
        attr_dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((dset_space_id = H5Screate_simple(ATTRIBUTE_ITERATE_TEST_DSET_SPACE_RANK, dset_dims, NULL)) < 0)
        TEST_ERROR
    if ((attr_space_id = H5Screate_simple(ATTRIBUTE_ITERATE_TEST_ATTR_SPACE_RANK, attr_dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, ATTRIBUTE_ITERATE_TEST_DSET_NAME, dset_dtype,
            dset_space_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset\n");
        goto error;
    }

    if ((attr_id = H5Acreate2(dset_id, ATTRIBUTE_ITERATE_TEST_ATTR_NAME, attr_dtype,
            attr_space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    if ((attr_id2 = H5Acreate2(dset_id, ATTRIBUTE_ITERATE_TEST_ATTR_NAME2, attr_dtype,
            attr_space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    if ((attr_id3 = H5Acreate2(dset_id, ATTRIBUTE_ITERATE_TEST_ATTR_NAME3, attr_dtype,
            attr_space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    if ((attr_id4 = H5Acreate2(dset_id, ATTRIBUTE_ITERATE_TEST_ATTR_NAME4, attr_dtype,
            attr_space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attributes have been created */
    if ((attr_exists = H5Aexists(dset_id, ATTRIBUTE_ITERATE_TEST_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    if ((attr_exists = H5Aexists(dset_id, ATTRIBUTE_ITERATE_TEST_ATTR_NAME2)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    if ((attr_exists = H5Aexists(dset_id, ATTRIBUTE_ITERATE_TEST_ATTR_NAME3)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    if ((attr_exists = H5Aexists(dset_id, ATTRIBUTE_ITERATE_TEST_ATTR_NAME4)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    /*
     * NOTE: Pass a counter to the iteration callback to try to match up the
     * expected attributes with a given step throughout all of the following
     * iterations. Since the only information we can count on in the attribute
     * iteration callback is the attribute's name, we need some other way of
     * ensuring that the attributes are coming back in the correct order.
     */
    i = 0;

    TESTING_2("H5Aiterate by attribute name in increasing order")

    /* Test basic attribute iteration capability using both index types and both index orders */
    if (H5Aiterate2(dset_id, H5_INDEX_NAME, H5_ITER_INC, NULL, attr_iter_callback1, &i) < 0) {
        H5_FAILED();
        HDprintf("    H5Aiterate2 by index type name in increasing order failed\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aiterate by attribute name in decreasing order")

    if (H5Aiterate2(dset_id, H5_INDEX_NAME, H5_ITER_DEC, NULL, attr_iter_callback1, &i) < 0) {
        H5_FAILED();
        HDprintf("    H5Aiterate2 by index type name in decreasing order failed\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aiterate by creation order in increasing order")

    if (H5Aiterate2(dset_id, H5_INDEX_CRT_ORDER, H5_ITER_INC, NULL, attr_iter_callback1, &i) < 0) {
        H5_FAILED();
        HDprintf("    H5Aiterate2 by index type creation order in increasing order failed\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aiterate by creation order in decreasing order")

    if (H5Aiterate2(dset_id, H5_INDEX_CRT_ORDER, H5_ITER_DEC, NULL, attr_iter_callback1, &i) < 0) {
        H5_FAILED();
        HDprintf("    H5Aiterate2 by index type creation order in decreasing order failed\n");
        goto error;
    }

    PASSED();

    /*
     * Make sure to reset the special counter.
     */
    i = 0;

    TESTING_2("H5Aiterate_by_name by attribute name in increasing order")

    if (H5Aiterate_by_name(file_id, "/" ATTRIBUTE_TEST_GROUP_NAME "/" ATTRIBUTE_ITERATE_TEST_SUBGROUP_NAME "/" ATTRIBUTE_ITERATE_TEST_DSET_NAME,
            H5_INDEX_NAME, H5_ITER_INC, NULL, attr_iter_callback1, &i, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    H5Aiterate_by_name by index type name in increasing order failed\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aiterate_by_name by attribute name in decreasing order")

    if (H5Aiterate_by_name(file_id, "/" ATTRIBUTE_TEST_GROUP_NAME "/" ATTRIBUTE_ITERATE_TEST_SUBGROUP_NAME "/" ATTRIBUTE_ITERATE_TEST_DSET_NAME,
            H5_INDEX_NAME, H5_ITER_DEC, NULL, attr_iter_callback1, &i, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    H5Aiterate_by_name by index type name in decreasing order failed\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aiterate_by_name by creation order in increasing order")

    if (H5Aiterate_by_name(file_id, "/" ATTRIBUTE_TEST_GROUP_NAME "/" ATTRIBUTE_ITERATE_TEST_SUBGROUP_NAME "/" ATTRIBUTE_ITERATE_TEST_DSET_NAME,
            H5_INDEX_CRT_ORDER, H5_ITER_INC, NULL, attr_iter_callback1, &i, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    H5Aiterate_by_name by index type creation order in increasing order failed\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aiterate_by_name by creation order in decreasing order")

    if (H5Aiterate_by_name(file_id, "/" ATTRIBUTE_TEST_GROUP_NAME "/" ATTRIBUTE_ITERATE_TEST_SUBGROUP_NAME "/" ATTRIBUTE_ITERATE_TEST_DSET_NAME,
            H5_INDEX_CRT_ORDER, H5_ITER_DEC, NULL, attr_iter_callback1, &i, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    H5Aiterate_by_name by index type creation order in decreasing order failed\n");
        goto error;
    }

    /* XXX: Test the H5Aiterate index-saving capabilities */


    if (H5Sclose(dset_space_id) < 0)
        TEST_ERROR
    if (H5Sclose(attr_space_id) < 0)
        TEST_ERROR
    if (H5Tclose(dset_dtype) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id2) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id3) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id4) < 0)
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
        H5Sclose(dset_space_id);
        H5Sclose(attr_space_id);
        H5Tclose(dset_dtype);
        H5Tclose(attr_dtype);
        H5Aclose(attr_id);
        H5Aclose(attr_id2);
        H5Aclose(attr_id3);
        H5Aclose(attr_id4);
        H5Dclose(dset_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that an object's attributes can't
 * be iterated over when H5Aiterate(_by_name) is
 * passed invalid parameters.
 */
static int
test_attribute_iterate_invalid_params(void)
{
    hsize_t attr_dims[ATTRIBUTE_ITERATE_INVALID_PARAMS_TEST_ATTR_SPACE_RANK];
    herr_t  err_ret = -1;
    size_t  i;
    htri_t  attr_exists;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t   attr_id = H5I_INVALID_HID, attr_id2 = H5I_INVALID_HID, attr_id3 = H5I_INVALID_HID, attr_id4 = H5I_INVALID_HID;
    hid_t   attr_dtype = H5I_INVALID_HID;
    hid_t   attr_space_id = H5I_INVALID_HID;

    TESTING("attribute iteration with invalid parameters"); HDputs("");

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group\n");
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_ITERATE_INVALID_PARAMS_TEST_SUBGROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container subgroup\n");
        goto error;
    }

    if ((attr_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    for (i = 0; i < ATTRIBUTE_ITERATE_INVALID_PARAMS_TEST_ATTR_SPACE_RANK; i++)
        attr_dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((attr_space_id = H5Screate_simple(ATTRIBUTE_ITERATE_INVALID_PARAMS_TEST_ATTR_SPACE_RANK, attr_dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_ITERATE_INVALID_PARAMS_TEST_ATTR_NAME, attr_dtype,
            attr_space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    if ((attr_id2 = H5Acreate2(group_id, ATTRIBUTE_ITERATE_INVALID_PARAMS_TEST_ATTR_NAME2, attr_dtype,
            attr_space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    if ((attr_id3 = H5Acreate2(group_id, ATTRIBUTE_ITERATE_INVALID_PARAMS_TEST_ATTR_NAME3, attr_dtype,
            attr_space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    if ((attr_id4 = H5Acreate2(group_id, ATTRIBUTE_ITERATE_INVALID_PARAMS_TEST_ATTR_NAME4, attr_dtype,
            attr_space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attributes have been created */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_ITERATE_INVALID_PARAMS_TEST_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_ITERATE_INVALID_PARAMS_TEST_ATTR_NAME2)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_ITERATE_INVALID_PARAMS_TEST_ATTR_NAME3)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_ITERATE_INVALID_PARAMS_TEST_ATTR_NAME4)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    TESTING_2("H5Aiterate with an invalid loc_id")

    H5E_BEGIN_TRY {
        err_ret = H5Aiterate2(H5I_INVALID_HID, H5_INDEX_NAME, H5_ITER_INC, NULL, attr_iter_callback2, NULL);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    iterated over attributes using H5Aiterate with an invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aiterate with an invalid index type")

    H5E_BEGIN_TRY {
        err_ret = H5Aiterate2(group_id, H5_INDEX_UNKNOWN, H5_ITER_INC, NULL, attr_iter_callback2, NULL);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    iterated over attributes using H5Aiterate with an invalid index type!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Aiterate2(group_id, H5_INDEX_N, H5_ITER_INC, NULL, attr_iter_callback2, NULL);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    iterated over attributes using H5Aiterate with an invalid index type!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aiterate with an invalid index ordering")

    H5E_BEGIN_TRY {
        err_ret = H5Aiterate2(group_id, H5_INDEX_NAME, H5_ITER_UNKNOWN, NULL, attr_iter_callback2, NULL);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    iterated over attributes using H5Aiterate with an invalid index ordering!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Aiterate2(group_id, H5_INDEX_NAME, H5_ITER_N, NULL, attr_iter_callback2, NULL);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    iterated over attributes using H5Aiterate with an invalid index ordering!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aiterate_by_name with an invalid loc_id")

    H5E_BEGIN_TRY {
        err_ret = H5Aiterate_by_name(H5I_INVALID_HID, ".", H5_INDEX_NAME, H5_ITER_INC, NULL, attr_iter_callback2, NULL, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    iterated over attributes using H5Aiterate_by_name with an invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aiterate_by_name with an invalid object name")

    H5E_BEGIN_TRY {
        err_ret = H5Aiterate_by_name(group_id, NULL, H5_INDEX_NAME, H5_ITER_INC, NULL, attr_iter_callback2, NULL, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    iterated over attributes using H5Aiterate_by_name with an invalid object name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Aiterate_by_name(group_id, "", H5_INDEX_NAME, H5_ITER_INC, NULL, attr_iter_callback2, NULL, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    iterated over attributes using H5Aiterate_by_name with an invalid object name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aiterate_by_name with an invalid index type")

    H5E_BEGIN_TRY {
        err_ret = H5Aiterate_by_name(group_id, ".", H5_INDEX_UNKNOWN, H5_ITER_INC, NULL, attr_iter_callback2, NULL, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    iterated over attributes using H5Aiterate_by_name with an invalid index type!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Aiterate_by_name(group_id, ".", H5_INDEX_N, H5_ITER_INC, NULL, attr_iter_callback2, NULL, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    iterated over attributes using H5Aiterate_by_name with an invalid index type!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aiterate_by_name with an invalid index ordering")

    H5E_BEGIN_TRY {
        err_ret = H5Aiterate_by_name(group_id, ".", H5_INDEX_NAME, H5_ITER_UNKNOWN, NULL, attr_iter_callback2, NULL, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    iterated over attributes using H5Aiterate_by_name with an invalid index ordering!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Aiterate_by_name(group_id, ".", H5_INDEX_NAME, H5_ITER_N, NULL, attr_iter_callback2, NULL, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    iterated over attributes using H5Aiterate_by_name with an invalid index ordering!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Aiterate_by_name with an invalid LAPL")

    H5E_BEGIN_TRY {
        err_ret = H5Aiterate_by_name(group_id, ".", H5_INDEX_NAME, H5_ITER_INC, NULL, attr_iter_callback2, NULL, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    iterated over attributes using H5Aiterate_by_name with an invalid LAPL!\n");
        goto error;
    }

    if (H5Sclose(attr_space_id) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id2) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id3) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id4) < 0)
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
        H5Sclose(attr_space_id);
        H5Tclose(attr_dtype);
        H5Aclose(attr_id);
        H5Aclose(attr_id2);
        H5Aclose(attr_id3);
        H5Aclose(attr_id4);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that attribute iteration performed
 * on an object with no attributes attached to it is
 * not problematic.
 */
static int
test_attribute_iterate_0_attributes(void)
{
    hsize_t dset_dims[ATTRIBUTE_ITERATE_TEST_0_ATTRIBUTES_DSET_SPACE_RANK];
    size_t  i;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t   dset_id = H5I_INVALID_HID;
    hid_t   dset_dtype = H5I_INVALID_HID;
    hid_t   dset_space_id = H5I_INVALID_HID;

    TESTING("attribute iteration on object with 0 attributes")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group\n");
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_ITERATE_TEST_0_ATTRIBUTES_SUBGROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container subgroup\n");
        goto error;
    }

    if ((dset_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    for (i = 0; i < ATTRIBUTE_ITERATE_TEST_0_ATTRIBUTES_DSET_SPACE_RANK; i++)
        dset_dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((dset_space_id = H5Screate_simple(ATTRIBUTE_ITERATE_TEST_0_ATTRIBUTES_DSET_SPACE_RANK, dset_dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, ATTRIBUTE_ITERATE_TEST_0_ATTRIBUTES_DSET_NAME, dset_dtype,
            dset_space_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset\n");
        goto error;
    }

    if (H5Aiterate2(dset_id, H5_INDEX_NAME, H5_ITER_INC, NULL, attr_iter_callback2, NULL) < 0) {
        H5_FAILED();
        HDprintf("    H5Aiterate2 on object with 0 attributes failed\n");
        goto error;
    }

    if (H5Aiterate_by_name(group_id, ATTRIBUTE_ITERATE_TEST_0_ATTRIBUTES_DSET_NAME, H5_INDEX_NAME, H5_ITER_INC,
            NULL, attr_iter_callback2, NULL, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    H5Aiterate_by_name on object with 0 attributes failed\n");
        goto error;
    }

    if (H5Sclose(dset_space_id) < 0)
        TEST_ERROR
    if (H5Tclose(dset_dtype) < 0)
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
        H5Sclose(dset_space_id);
        H5Tclose(dset_dtype);
        H5Dclose(dset_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that an attribute can be deleted.
 */
static int
test_delete_attribute(void)
{
    hsize_t dims[ATTRIBUTE_DELETION_TEST_SPACE_RANK];
    size_t  i;
    htri_t  attr_exists;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID;
    hid_t   group_id = H5I_INVALID_HID;
    hid_t   attr_id = H5I_INVALID_HID;
    hid_t   attr_dtype = H5I_INVALID_HID;
    hid_t   space_id = H5I_INVALID_HID;

    TESTING("attribute deletion"); HDputs("");

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", ATTRIBUTE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_DELETION_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", ATTRIBUTE_DELETION_TEST_GROUP_NAME);
    }

    for (i = 0; i < ATTRIBUTE_DELETION_TEST_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((space_id = H5Screate_simple(ATTRIBUTE_DELETION_TEST_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    /* Test H5Adelete */
    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_DELETION_TEST_ATTR_NAME, attr_dtype,
            space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_DELETION_TEST_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute didn't exists\n");
        goto error;
    }

    TESTING_2("H5Adelete")

    /* Delete the attribute */
    if (H5Adelete(group_id, ATTRIBUTE_DELETION_TEST_ATTR_NAME) < 0) {
        H5_FAILED();
        HDprintf("    failed to delete attribute\n");
        goto error;
    }

    /* Verify the attribute has been deleted */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_DELETION_TEST_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (attr_exists) {
        H5_FAILED();
        HDprintf("    attribute exists!\n");
        goto error;
    }

    if (H5Aclose(attr_id) < 0)
        TEST_ERROR

    PASSED();

    TESTING_2("H5Adelete_by_name")

    /* Test H5Adelete_by_name */
    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_DELETION_TEST_ATTR_NAME, attr_dtype,
            space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_DELETION_TEST_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute didn't exists\n");
        goto error;
    }

    /* Delete the attribute */
    if (H5Adelete_by_name(container_group, ATTRIBUTE_DELETION_TEST_GROUP_NAME, ATTRIBUTE_DELETION_TEST_ATTR_NAME, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    failed to delete attribute\n");
        goto error;
    }

    /* Verify the attribute has been deleted */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_DELETION_TEST_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (attr_exists) {
        H5_FAILED();
        HDprintf("    attribute exists!\n");
        goto error;
    }

    if (H5Aclose(attr_id) < 0)
        TEST_ERROR

    PASSED();

    TESTING_2("H5Adelete_by_idx")

    /* Test H5Adelete_by_idx */
    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_DELETION_TEST_ATTR_NAME, attr_dtype,
            space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_DELETION_TEST_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute didn't exists\n");
        goto error;
    }

    if (H5Adelete_by_idx(container_group, ATTRIBUTE_DELETION_TEST_GROUP_NAME, H5_INDEX_NAME, H5_ITER_INC, 0, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    failed to delete attribute by index number\n");
        goto error;
    }

    /* Verify the attribute has been deleted */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_DELETION_TEST_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (attr_exists) {
        H5_FAILED();
        HDprintf("    attribute exists!\n");
        goto error;
    }

    if (H5Sclose(space_id) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype) < 0)
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
        H5Sclose(space_id);
        H5Tclose(attr_dtype);
        H5Aclose(attr_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that an attribute can't be deleted
 * when H5Adelete(_by_name/_by_idx) is passed invalid
 * parameters.
 */
static int
test_delete_attribute_invalid_params(void)
{
    hsize_t dims[ATTRIBUTE_DELETION_INVALID_PARAMS_TEST_SPACE_RANK];
    herr_t  err_ret = -1;
    size_t  i;
    htri_t  attr_exists;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID;
    hid_t   group_id = H5I_INVALID_HID;
    hid_t   attr_id = H5I_INVALID_HID;
    hid_t   attr_dtype = H5I_INVALID_HID;
    hid_t   space_id = H5I_INVALID_HID;

    TESTING("attribute deletion with invalid parameters"); HDputs("");

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", ATTRIBUTE_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, ATTRIBUTE_DELETION_INVALID_PARAMS_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", ATTRIBUTE_DELETION_INVALID_PARAMS_TEST_GROUP_NAME);
    }

    for (i = 0; i < ATTRIBUTE_DELETION_INVALID_PARAMS_TEST_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((space_id = H5Screate_simple(ATTRIBUTE_DELETION_INVALID_PARAMS_TEST_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((attr_id = H5Acreate2(group_id, ATTRIBUTE_DELETION_INVALID_PARAMS_TEST_ATTR_NAME, attr_dtype,
            space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(group_id, ATTRIBUTE_DELETION_INVALID_PARAMS_TEST_ATTR_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute didn't exists\n");
        goto error;
    }

    TESTING_2("H5Adelete with an invalid loc_id")

    H5E_BEGIN_TRY {
        err_ret = H5Adelete(H5I_INVALID_HID, ATTRIBUTE_DELETION_INVALID_PARAMS_TEST_ATTR_NAME);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    deleted an attribute using H5Adelete with an invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Adelete with an invalid attribute name")

    H5E_BEGIN_TRY {
        err_ret = H5Adelete(group_id, NULL);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    deleted an attribute using H5Adelete with an invalid attribute name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Adelete(group_id, "");
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    deleted an attribute using H5Adelete with an invalid attribute name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Adelete_by_name with an invalid loc_id")

    H5E_BEGIN_TRY {
        err_ret = H5Adelete_by_name(H5I_INVALID_HID, ".", ATTRIBUTE_DELETION_INVALID_PARAMS_TEST_ATTR_NAME, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    deleted an attribute using H5Adelete_by_name with an invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Adelete_by_name with an invalid object name")

    H5E_BEGIN_TRY {
        err_ret = H5Adelete_by_name(group_id, NULL, ATTRIBUTE_DELETION_INVALID_PARAMS_TEST_ATTR_NAME, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    deleted an attribute using H5Adelete_by_name with an invalid object name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Adelete_by_name(group_id, "", ATTRIBUTE_DELETION_INVALID_PARAMS_TEST_ATTR_NAME, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    deleted an attribute using H5Adelete_by_name with an invalid object name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Adelete_by_name with an invalid attribute name")

    H5E_BEGIN_TRY {
        err_ret = H5Adelete_by_name(group_id, ".", NULL, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    deleted an attribute using H5Adelete_by_name with an invalid attribute name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Adelete_by_name(group_id, ".", "", H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    deleted an attribute using H5Adelete_by_name with an invalid attribute name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Adelete_by_name with an invalid LAPL")

    H5E_BEGIN_TRY {
        err_ret = H5Adelete_by_name(group_id, ".", ATTRIBUTE_DELETION_INVALID_PARAMS_TEST_ATTR_NAME, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    deleted an attribute using H5Adelete_by_name with an invalid LAPL!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Adelete_by_idx with an invalid loc_id")

    H5E_BEGIN_TRY {
        err_ret = H5Adelete_by_idx(H5I_INVALID_HID, ".", H5_INDEX_NAME, H5_ITER_INC, 0, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    deleted an attribute using H5Adelete_by_idx with an invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Adelete_by_idx with an invalid object name")

    H5E_BEGIN_TRY {
        err_ret = H5Adelete_by_idx(group_id, NULL, H5_INDEX_NAME, H5_ITER_INC, 0, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    deleted an attribute using H5Adelete_by_idx with an invalid object name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Adelete_by_idx(group_id, "", H5_INDEX_NAME, H5_ITER_INC, 0, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    deleted an attribute using H5Adelete_by_idx with an invalid object name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Adelete_by_idx with an invalid index type")

    H5E_BEGIN_TRY {
        err_ret = H5Adelete_by_idx(group_id, ".", H5_INDEX_UNKNOWN, H5_ITER_INC, 0, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    deleted an attribute using H5Adelete_by_idx with an invalid index type!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Adelete_by_idx(group_id, ".", H5_INDEX_N, H5_ITER_INC, 0, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    deleted an attribute using H5Adelete_by_idx with an invalid index type!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Adelete_by_idx with an invalid index ordering")

    H5E_BEGIN_TRY {
        err_ret = H5Adelete_by_idx(group_id, ".", H5_INDEX_NAME, H5_ITER_UNKNOWN, 0, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    deleted an attribute using H5Adelete_by_idx with an invalid index ordering!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Adelete_by_idx(group_id, ".", H5_INDEX_NAME, H5_ITER_N, 0, H5P_DEFAULT);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    deleted an attribute using H5Adelete_by_idx with an invalid index ordering!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Adelete_by_idx with an invalid LAPL")

    H5E_BEGIN_TRY {
        err_ret = H5Adelete_by_idx(group_id, ".", H5_INDEX_NAME, H5_ITER_INC, 0, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    deleted an attribute using H5Adelete_by_idx with an invalid LAPL!\n");
        goto error;
    }

    if (H5Sclose(space_id) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype) < 0)
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
        H5Sclose(space_id);
        H5Tclose(attr_dtype);
        H5Aclose(attr_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test for H5Aexists.
 */
static int
test_attribute_exists(void)
{
    TESTING("H5Aexists")

    SKIPPED();

    return 0;
}

/*
 * A test to ensure that H5Aexists will fail when
 * given invalid parameters.
 */
static int
test_attribute_exists_invalid_params(void)
{
    TESTING("H5Aexists with invalid parameters")

    SKIPPED();

    return 0;
}

/*
 * A test to check that the number of attributes attached
 * to an object (group, dataset, datatype) can be retrieved.
 *
 * XXX: Cover all of the cases and move to H5O tests.
 */
static int
test_get_number_attributes(void)
{
    H5O_info_t obj_info;
    hsize_t    dims[ATTRIBUTE_GET_NUM_ATTRS_TEST_SPACE_RANK];
    size_t     i;
    htri_t     attr_exists;
    hid_t      file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t      container_group = H5I_INVALID_HID;
    hid_t      attr_id = H5I_INVALID_HID;
    hid_t      attr_dtype = H5I_INVALID_HID;
    hid_t      space_id = H5I_INVALID_HID;

    TESTING("retrieve the number of attributes on an object")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, ATTRIBUTE_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group\n");
        goto error;
    }

    for (i = 0; i < ATTRIBUTE_GET_NUM_ATTRS_TEST_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((space_id = H5Screate_simple(ATTRIBUTE_GET_NUM_ATTRS_TEST_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((attr_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((attr_id = H5Acreate2(container_group, ATTRIBUTE_GET_NUM_ATTRS_TEST_ATTRIBUTE_NAME, attr_dtype,
            space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create attribute\n");
        goto error;
    }

    /* Verify the attribute has been created */
    if ((attr_exists = H5Aexists(container_group, ATTRIBUTE_GET_NUM_ATTRS_TEST_ATTRIBUTE_NAME)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't determine if attribute exists\n");
        goto error;
    }

    if (!attr_exists) {
        H5_FAILED();
        HDprintf("    attribute did not exist\n");
        goto error;
    }

    /* Now get the number of attributes from the group */
    if (H5Oget_info2(container_group, &obj_info, H5O_INFO_ALL) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve root group info using H5Oget_info2\n");
        goto error;
    }

    if (obj_info.num_attrs < 1) {
        H5_FAILED();
        HDprintf("    invalid number of attributes received\n");
        goto error;
    }

    if (H5Oget_info_by_name2(file_id, "/" ATTRIBUTE_TEST_GROUP_NAME, &obj_info, H5O_INFO_ALL, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve root group info using H5Oget_info_by_name2\n");
        goto error;
    }

    if (obj_info.num_attrs < 1) {
        H5_FAILED();
        HDprintf("    invalid number of attributes received\n");
        goto error;
    }

    if (H5Oget_info_by_idx2(file_id, ".", H5_INDEX_NAME, H5_ITER_INC, 0, &obj_info, H5O_INFO_ALL, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve root group info using H5Oget_info_by_idx2\n");
        goto error;
    }

    if (obj_info.num_attrs < 1) {
        H5_FAILED();
        HDprintf("    invalid number of attributes received\n");
        goto error;
    }

    if (H5Sclose(space_id) < 0)
        TEST_ERROR
    if (H5Tclose(attr_dtype) < 0)
        TEST_ERROR
    if (H5Aclose(attr_id) < 0)
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
        H5Sclose(space_id);
        H5Tclose(attr_dtype);
        H5Aclose(attr_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

static herr_t
attr_iter_callback1(hid_t location_id, const char *attr_name, const H5A_info_t *ainfo, void *op_data)
{
    size_t *i = (size_t *) op_data;
    size_t  counter_val = *((size_t *) op_data);
    herr_t  ret_val = 0;

    UNUSED(location_id);
    UNUSED(ainfo);

    if (!HDstrcmp(attr_name, ATTRIBUTE_ITERATE_TEST_ATTR_NAME) &&
            (counter_val == 0 || counter_val == 7 || counter_val == 8 || counter_val == 15)) {
        goto done;
    }
    else if (!HDstrcmp(attr_name, ATTRIBUTE_ITERATE_TEST_ATTR_NAME2) &&
            (counter_val == 1 || counter_val == 6 || counter_val == 9 || counter_val == 14)) {
        goto done;
    }
    else if (!HDstrcmp(attr_name, ATTRIBUTE_ITERATE_TEST_ATTR_NAME3) &&
            (counter_val == 2 || counter_val == 5 || counter_val == 10 || counter_val == 13)) {
        goto done;
    }
    else if (!HDstrcmp(attr_name, ATTRIBUTE_ITERATE_TEST_ATTR_NAME4) &&
            (counter_val == 3 || counter_val == 4 || counter_val == 11 || counter_val == 12)) {
        goto done;
    }

    HDprintf("    attribute name '%s' didn't match known names or came in the incorrect order\n", attr_name);

    ret_val = -1;

done:
    (*i)++;

    return ret_val;
}

static herr_t
attr_iter_callback2(hid_t location_id, const char *attr_name, const H5A_info_t *ainfo, void *op_data)
{
    UNUSED(location_id);
    UNUSED(attr_name);
    UNUSED(ainfo);
    UNUSED(op_data);

    return 0;
}

int
vol_attribute_test(void)
{
    size_t i;
    int    nerrors;

    HDprintf("**********************************************\n");
    HDprintf("*                                            *\n");
    HDprintf("*            VOL Attribute Tests             *\n");
    HDprintf("*                                            *\n");
    HDprintf("**********************************************\n\n");

    for (i = 0, nerrors = 0; i < ARRAY_LENGTH(attribute_tests); i++) {
        nerrors += (*attribute_tests[i])() ? 1 : 0;
    }

    HDprintf("\n");

done:
    return nerrors;
}
