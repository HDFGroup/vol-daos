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

#include "vol_dataset_test.h"

/*
 * XXX: H5Dread_chunk/H5Dwrite_chunk, H5Dfill/scatter/gather
 */

static int test_create_dataset_under_root(void);
static int test_create_dataset_under_existing_group(void);
static int test_create_dataset_invalid_params(void);
static int test_create_anonymous_dataset(void);
static int test_create_anonymous_dataset_invalid_params(void);
static int test_create_dataset_null_space(void);
static int test_create_dataset_scalar_space(void);
static int test_create_dataset_random_shapes(void);
static int test_create_dataset_predefined_types(void);
static int test_create_dataset_string_types(void);
static int test_create_dataset_compound_types(void);
static int test_create_dataset_enum_types(void);
static int test_create_dataset_array_types(void);
static int test_create_dataset_creation_properties(void);
static int test_create_many_dataset(void);
static int test_open_dataset(void);
static int test_open_dataset_invalid_params(void);
static int test_close_dataset_invalid_params(void);
static int test_get_dataset_space_and_type(void);
static int test_get_dataset_space_and_type_invalid_params(void);
static int test_get_dataset_space_status(void);
static int test_get_dataset_space_status_invalid_params(void);
static int test_dataset_property_lists(void);
static int test_get_dataset_storage_size(void);
static int test_get_dataset_storage_size_invalid_params(void);
static int test_get_dataset_chunk_storage_size(void);
static int test_get_dataset_chunk_storage_size_invalid_params(void);
static int test_get_dataset_offset(void);
static int test_get_dataset_offset_invalid_params(void);
static int test_read_dataset_small_all(void);
static int test_read_dataset_small_hyperslab(void);
static int test_read_dataset_small_point_selection(void);
#ifndef NO_LARGE_TESTS
static int test_read_dataset_large_all(void);
static int test_read_dataset_large_hyperslab(void);
static int test_read_dataset_large_point_selection(void);
#endif
static int test_read_dataset_invalid_params(void);
static int test_write_dataset_small_all(void);
static int test_write_dataset_small_hyperslab(void);
static int test_write_dataset_small_point_selection(void);
#ifndef NO_LARGE_TESTS
static int test_write_dataset_large_all(void);
static int test_write_dataset_large_hyperslab(void);
static int test_write_dataset_large_point_selection(void);
#endif
static int test_write_dataset_data_verification(void);
static int test_write_dataset_invalid_params(void);
static int test_dataset_iterate(void);
static int test_dataset_iterate_empty_selection(void);
static int test_dataset_iterate_invalid_params(void);
static int test_dataset_set_extent_chunked_unlimited(void);
static int test_dataset_set_extent_chunked_fixed(void);
static int test_dataset_set_extent_invalid_params(void);
static int test_flush_dataset(void);
static int test_flush_dataset_invalid_params(void);
static int test_refresh_dataset(void);
static int test_refresh_dataset_invalid_params(void);

/*
 * The array of dataset tests to be performed.
 */
static int (*dataset_tests[])(void) = {
        test_create_dataset_under_root,
        test_create_dataset_under_existing_group,
        test_create_dataset_invalid_params,
        test_create_anonymous_dataset,
        test_create_anonymous_dataset_invalid_params,
        test_create_dataset_null_space,
        test_create_dataset_scalar_space,
        test_create_dataset_random_shapes,
        test_create_dataset_predefined_types,
        test_create_dataset_string_types,
        test_create_dataset_compound_types,
        test_create_dataset_enum_types,
        test_create_dataset_array_types,
        test_create_dataset_creation_properties,
        test_create_many_dataset,
        test_open_dataset,
        test_open_dataset_invalid_params,
        test_close_dataset_invalid_params,
        test_get_dataset_space_and_type,
        test_get_dataset_space_and_type_invalid_params,
        test_get_dataset_space_status,
        test_get_dataset_space_status_invalid_params,
        test_dataset_property_lists,
        test_get_dataset_storage_size,
        test_get_dataset_storage_size_invalid_params,
        test_get_dataset_chunk_storage_size,
        test_get_dataset_chunk_storage_size_invalid_params,
        test_get_dataset_offset,
        test_get_dataset_offset_invalid_params,
        test_read_dataset_small_all,
        test_read_dataset_small_hyperslab,
        test_read_dataset_small_point_selection,
#ifndef NO_LARGE_TESTS
        test_read_dataset_large_all,
        test_read_dataset_large_hyperslab,
        test_read_dataset_large_point_selection,
#endif
        test_read_dataset_invalid_params,
        test_write_dataset_small_all,
        test_write_dataset_small_hyperslab,
        test_write_dataset_small_point_selection,
#ifndef NO_LARGE_TESTS
        test_write_dataset_large_all,
        test_write_dataset_large_hyperslab,
        test_write_dataset_large_point_selection,
#endif
        test_write_dataset_data_verification,
        test_write_dataset_invalid_params,
        test_dataset_iterate,
        test_dataset_iterate_empty_selection,
        test_dataset_iterate_invalid_params,
        test_dataset_set_extent_chunked_unlimited,
        test_dataset_set_extent_chunked_fixed,
        test_dataset_set_extent_invalid_params,
        test_flush_dataset,
        test_flush_dataset_invalid_params,
        test_refresh_dataset,
        test_refresh_dataset_invalid_params,
};

/*
 * A test to check that a dataset can be
 * created under the root group.
 */
static int
test_create_dataset_under_root(void)
{
    hsize_t dims[DATASET_CREATE_UNDER_ROOT_SPACE_RANK];
    size_t  i;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   dset_id = H5I_INVALID_HID;
    hid_t   dset_dtype = H5I_INVALID_HID;
    hid_t   fspace_id = H5I_INVALID_HID;

    TESTING("dataset creation under root group")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    for (i = 0; i < DATASET_CREATE_UNDER_ROOT_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((fspace_id = H5Screate_simple(DATASET_CREATE_UNDER_ROOT_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    /* Create the Dataset under the root group of the file */
    if ((dset_id = H5Dcreate2(file_id, DATASET_CREATE_UNDER_ROOT_DSET_NAME, dset_dtype, fspace_id,
            H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_CREATE_UNDER_ROOT_DSET_NAME);
        goto error;
    }

    if (H5Sclose(fspace_id) < 0)
        TEST_ERROR
    if (H5Tclose(dset_dtype) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id) < 0)
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
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that a dataset can be created
 * under a group that is not the root group.
 */
static int
test_create_dataset_under_existing_group(void)
{
    hsize_t dims[DATASET_CREATE_UNDER_EXISTING_SPACE_RANK];
    size_t  i;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t   dset_id = H5I_INVALID_HID;
    hid_t   dset_dtype = H5I_INVALID_HID;
    hid_t   fspace_id = H5I_INVALID_HID;

    TESTING("dataset creation under an existing group")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_CREATE_UNDER_EXISTING_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", DATASET_CREATE_UNDER_EXISTING_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < DATASET_CREATE_UNDER_EXISTING_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((fspace_id = H5Screate_simple(DATASET_CREATE_UNDER_EXISTING_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, DATASET_CREATE_UNDER_EXISTING_DSET_NAME, dset_dtype, fspace_id,
            H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_CREATE_UNDER_EXISTING_DSET_NAME);
        goto error;
    }

    if (H5Sclose(fspace_id) < 0)
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
        H5Sclose(fspace_id);
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
 * A test to check that a dataset can't be created
 * when H5Dcreate is passed invalid parameters.
 */
static int
test_create_dataset_invalid_params(void)
{
    hsize_t dims[DATASET_CREATE_INVALID_PARAMS_SPACE_RANK];
    size_t  i;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t   dset_id = H5I_INVALID_HID;
    hid_t   dset_dtype = H5I_INVALID_HID;
    hid_t   fspace_id = H5I_INVALID_HID;

    TESTING("H5Dcreate with invalid parameters"); HDputs("");

    TESTING_2("H5Dcreate with an invalid loc_id")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_CREATE_INVALID_PARAMS_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", DATASET_CREATE_INVALID_PARAMS_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < DATASET_CREATE_INVALID_PARAMS_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((fspace_id = H5Screate_simple(DATASET_CREATE_INVALID_PARAMS_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    H5E_BEGIN_TRY {
        dset_id = H5Dcreate2(H5I_INVALID_HID, DATASET_CREATE_INVALID_PARAMS_DSET_NAME, dset_dtype, fspace_id,
                H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (dset_id >= 0) {
        H5_FAILED();
        HDprintf("    created dataset using H5Dcreate with an invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dcreate with an invalid dataset name")

    H5E_BEGIN_TRY {
        dset_id = H5Dcreate2(group_id, NULL, dset_dtype, fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (dset_id >= 0) {
        H5_FAILED();
        HDprintf("    created dataset using H5Dcreate with an invalid dataset name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        dset_id = H5Dcreate2(group_id, "", dset_dtype, fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (dset_id >= 0) {
        H5_FAILED();
        HDprintf("    created dataset using H5Dcreate with an invalid dataset name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dcreate with an invalid datatype")

    H5E_BEGIN_TRY {
        dset_id = H5Dcreate2(group_id, DATASET_CREATE_INVALID_PARAMS_DSET_NAME, H5I_INVALID_HID, fspace_id,
                H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (dset_id >= 0) {
        H5_FAILED();
        HDprintf("    created dataset using H5Dcreate with an invalid datatype!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dcreate with an invalid dataspace")

    H5E_BEGIN_TRY {
        dset_id = H5Dcreate2(group_id, DATASET_CREATE_INVALID_PARAMS_DSET_NAME, dset_dtype, H5I_INVALID_HID,
                H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (dset_id >= 0) {
        H5_FAILED();
        HDprintf("    created dataset using H5Dcreate with an invalid dataspace!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dcreate with an invalid LCPL")

    H5E_BEGIN_TRY {
        dset_id = H5Dcreate2(group_id, DATASET_CREATE_INVALID_PARAMS_DSET_NAME, dset_dtype, fspace_id,
                H5I_INVALID_HID, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (dset_id >= 0) {
        H5_FAILED();
        HDprintf("    created dataset using H5Dcreate with an invalid LCPL!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dcreate with an invalid DCPL")

    H5E_BEGIN_TRY {
        dset_id = H5Dcreate2(group_id, DATASET_CREATE_INVALID_PARAMS_DSET_NAME, dset_dtype, fspace_id,
                H5P_DEFAULT, H5I_INVALID_HID, H5P_DEFAULT);
    } H5E_END_TRY;

    if (dset_id >= 0) {
        H5_FAILED();
        HDprintf("    created dataset using H5Dcreate with an invalid DCPL!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dcreate with an invalid DAPL")

    H5E_BEGIN_TRY {
        dset_id = H5Dcreate2(group_id, DATASET_CREATE_INVALID_PARAMS_DSET_NAME, dset_dtype, fspace_id,
                H5P_DEFAULT, H5P_DEFAULT, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (dset_id >= 0) {
        H5_FAILED();
        HDprintf("    created dataset using H5Dcreate with an invalid DAPL!\n");
        goto error;
    }

    if (H5Sclose(fspace_id) < 0)
        TEST_ERROR
    if (H5Tclose(dset_dtype) < 0)
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
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that an anonymous dataset can be created.
 */
static int
test_create_anonymous_dataset(void)
{
    hsize_t dims[DATASET_CREATE_ANONYMOUS_SPACE_RANK];
    size_t  i;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t   dset_id = H5I_INVALID_HID;
    hid_t   dset_dtype = H5I_INVALID_HID;
    hid_t   fspace_id = H5I_INVALID_HID;

    TESTING("anonymous dataset creation")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_CREATE_ANONYMOUS_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", DATASET_CREATE_ANONYMOUS_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < DATASET_CREATE_ANONYMOUS_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((fspace_id = H5Screate_simple(DATASET_CREATE_ANONYMOUS_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate_anon(group_id, dset_dtype, fspace_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create anonymous dataset\n");
        goto error;
    }

    if (H5Olink(dset_id, group_id, DATASET_CREATE_ANONYMOUS_DATASET_NAME, H5P_DEFAULT, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDprintf("    couldn't link anonymous dataset into file structure\n");
        goto error;
    }

    if (H5Sclose(fspace_id) < 0)
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
        H5Sclose(fspace_id);
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
 * A test to check that an anonymous dataset can't
 * be created when H5Dcreate_anon is passed invalid
 * parameters.
 */
static int
test_create_anonymous_dataset_invalid_params(void)
{
    hsize_t dims[DATASET_CREATE_ANONYMOUS_INVALID_PARAMS_SPACE_RANK];
    size_t  i;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t   dset_id = H5I_INVALID_HID;
    hid_t   dset_dtype = H5I_INVALID_HID;
    hid_t   fspace_id = H5I_INVALID_HID;

    TESTING("anonymous dataset creation with invalid parameters"); HDputs("");

    TESTING_2("H5Dcreate_anon with an invalid loc_id")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_CREATE_ANONYMOUS_INVALID_PARAMS_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", DATASET_CREATE_ANONYMOUS_INVALID_PARAMS_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < DATASET_CREATE_ANONYMOUS_INVALID_PARAMS_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((fspace_id = H5Screate_simple(DATASET_CREATE_ANONYMOUS_INVALID_PARAMS_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    H5E_BEGIN_TRY {
        dset_id = H5Dcreate_anon(H5I_INVALID_HID, dset_dtype, fspace_id, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (dset_id >= 0) {
        H5_FAILED();
        HDprintf("    created anonymous dataset using an invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dcreate_anon with an invalid dataset datatype")

    H5E_BEGIN_TRY {
        dset_id = H5Dcreate_anon(group_id, H5I_INVALID_HID, fspace_id, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (dset_id >= 0) {
        H5_FAILED();
        HDprintf("    created anonymous dataset using an invalid dataset datatype!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dcreate_anon with an invalid dataset dataspace")

    H5E_BEGIN_TRY {
        dset_id = H5Dcreate_anon(group_id, dset_dtype, H5I_INVALID_HID, H5P_DEFAULT, H5P_DEFAULT);
    } H5E_END_TRY;

    if (dset_id >= 0) {
        H5_FAILED();
        HDprintf("    created anonymous dataset using an invalid dataset dataspace!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dcreate_anon with an invalid DCPL")

    H5E_BEGIN_TRY {
        dset_id = H5Dcreate_anon(group_id, dset_dtype, fspace_id, H5I_INVALID_HID, H5P_DEFAULT);
    } H5E_END_TRY;

    if (dset_id >= 0) {
        H5_FAILED();
        HDprintf("    created anonymous dataset using an invalid DCPL!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dcreate_anon with an invalid DAPL")

    H5E_BEGIN_TRY {
        dset_id = H5Dcreate_anon(group_id, dset_dtype, fspace_id, H5P_DEFAULT, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (dset_id >= 0) {
        H5_FAILED();
        HDprintf("    created anonymous dataset using an invalid DAPL!\n");
        goto error;
    }

    if (H5Sclose(fspace_id) < 0)
        TEST_ERROR
    if (H5Tclose(dset_dtype) < 0)
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
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that creating a dataset with a NULL
 * dataspace is not problematic.
 */
static int
test_create_dataset_null_space(void)
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t dset_id = H5I_INVALID_HID;
    hid_t dset_dtype = H5I_INVALID_HID;
    hid_t fspace_id = H5I_INVALID_HID;

    TESTING("dataset creation with a NULL dataspace")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_CREATE_NULL_DATASPACE_TEST_SUBGROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container subgroup '%s'\n", DATASET_CREATE_NULL_DATASPACE_TEST_SUBGROUP_NAME);
        goto error;
    }

    if ((fspace_id = H5Screate(H5S_NULL)) < 0)
        TEST_ERROR

    if ((dset_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, DATASET_CREATE_NULL_DATASPACE_TEST_DSET_NAME, dset_dtype, fspace_id,
            H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_CREATE_NULL_DATASPACE_TEST_DSET_NAME);
        goto error;
    }

    if (H5Dclose(dset_id) < 0)
        TEST_ERROR

    if ((dset_id = H5Dopen2(group_id, DATASET_CREATE_NULL_DATASPACE_TEST_DSET_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open dataset '%s'\n", DATASET_CREATE_NULL_DATASPACE_TEST_DSET_NAME);
        goto error;
    }

    if (H5Sclose(fspace_id) < 0)
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
        H5Sclose(fspace_id);
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
 * A test to check that creating a dataset with a scalar
 * dataspace is not problematic.
 */
static int
test_create_dataset_scalar_space(void)
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t dset_id = H5I_INVALID_HID;
    hid_t dset_dtype = H5I_INVALID_HID;
    hid_t fspace_id = H5I_INVALID_HID;

    TESTING("dataset creation with a SCALAR dataspace")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_CREATE_SCALAR_DATASPACE_TEST_SUBGROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container subgroup '%s'\n", DATASET_CREATE_SCALAR_DATASPACE_TEST_SUBGROUP_NAME);
        goto error;
    }

    if ((fspace_id = H5Screate(H5S_SCALAR)) < 0)
        TEST_ERROR

    if ((dset_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, DATASET_CREATE_SCALAR_DATASPACE_TEST_DSET_NAME, dset_dtype, fspace_id,
            H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_CREATE_SCALAR_DATASPACE_TEST_DSET_NAME);
        goto error;
    }

    if (H5Dclose(dset_id) < 0)
        TEST_ERROR

    if ((dset_id = H5Dopen2(group_id, DATASET_CREATE_SCALAR_DATASPACE_TEST_DSET_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open dataset '%s'\n", DATASET_CREATE_SCALAR_DATASPACE_TEST_DSET_NAME);
        goto error;
    }

    if (H5Sclose(fspace_id) < 0)
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
        H5Sclose(fspace_id);
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
 * A test to check that a dataset can be created with
 * a variety of different dataspace shapes.
 */
static int
test_create_dataset_random_shapes(void)
{
    hsize_t *dims = NULL;
    size_t   i;
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t    dset_id = H5I_INVALID_HID, space_id = H5I_INVALID_HID;
    hid_t    dset_dtype = H5I_INVALID_HID;

    TESTING("dataset creation with random dimension sizes")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file\n");
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group\n");
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_SHAPE_TEST_SUBGROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group\n");
        goto error;
    }

    if ((dset_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    for (i = 0; i < DATASET_SHAPE_TEST_NUM_ITERATIONS; i++) {
        size_t j;
        char   name[100];
        int    ndims = rand() % DATASET_SHAPE_TEST_MAX_DIMS + 1;

        if (NULL == (dims = (hsize_t *) HDmalloc((size_t) ndims * sizeof(*dims)))) {
            H5_FAILED();
            HDprintf("    couldn't allocate space for dataspace dimensions\n");
            goto error;
        }

        for (j = 0; j < (size_t) ndims; j++)
            dims[j] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

        if ((space_id = H5Screate_simple(ndims, dims, NULL)) < 0) {
            H5_FAILED();
            HDprintf("    couldn't create dataspace\n");
            goto error;
        }

        HDsprintf(name, "%s%zu", DATASET_SHAPE_TEST_DSET_BASE_NAME, i + 1);

        if ((dset_id = H5Dcreate2(group_id, name, dset_dtype, space_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDprintf("    couldn't create dataset\n");
            goto error;
        }

        if (dims) {
            HDfree(dims);
            dims = NULL;
        }

        if (H5Sclose(space_id) < 0)
            TEST_ERROR
        if (H5Dclose(dset_id) < 0)
            TEST_ERROR
    }

    if (H5Tclose(dset_dtype) < 0)
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
        if (dims) HDfree(dims);
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

/*
 * A test to check that a dataset can be created using
 * each of the predefined integer and floating-point
 * datatypes.
 */
static int
test_create_dataset_predefined_types(void)
{
    size_t i;
    hid_t  file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t  container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t  fspace_id = H5I_INVALID_HID;
    hid_t  dset_id = H5I_INVALID_HID;
    hid_t  predefined_type_test_table[] = {
            H5T_STD_U8LE,   H5T_STD_U8BE,   H5T_STD_I8LE,   H5T_STD_I8BE,
            H5T_STD_U16LE,  H5T_STD_U16BE,  H5T_STD_I16LE,  H5T_STD_I16BE,
            H5T_STD_U32LE,  H5T_STD_U32BE,  H5T_STD_I32LE,  H5T_STD_I32BE,
            H5T_STD_U64LE,  H5T_STD_U64BE,  H5T_STD_I64LE,  H5T_STD_I64BE,
            H5T_IEEE_F32LE, H5T_IEEE_F32BE, H5T_IEEE_F64LE, H5T_IEEE_F64BE
    };

    TESTING("dataset creation with predefined datatypes")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_PREDEFINED_TYPE_TEST_SUBGROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create sub-container group '%s'\n", DATASET_PREDEFINED_TYPE_TEST_SUBGROUP_NAME);
        goto error;
    }

    for (i = 0; i < ARRAY_LENGTH(predefined_type_test_table); i++) {
        hsize_t dims[DATASET_PREDEFINED_TYPE_TEST_SPACE_RANK];
        size_t  j;
        char    name[100];

        for (j = 0; j < DATASET_PREDEFINED_TYPE_TEST_SPACE_RANK; j++)
            dims[j] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

        if ((fspace_id = H5Screate_simple(DATASET_PREDEFINED_TYPE_TEST_SPACE_RANK, dims, NULL)) < 0)
            TEST_ERROR

        HDsprintf(name, "%s%zu", DATASET_PREDEFINED_TYPE_TEST_BASE_NAME, i);

        if ((dset_id = H5Dcreate2(group_id, name, predefined_type_test_table[i], fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDprintf("    couldn't create dataset '%s'\n", name);
            goto error;
        }

        if (H5Sclose(fspace_id) < 0)
            TEST_ERROR
        if (H5Dclose(dset_id) < 0)
            TEST_ERROR

        if ((dset_id = H5Dopen2(group_id, name, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDprintf("    failed to open dataset '%s'\n", name);
            goto error;
        }

        if (H5Dclose(dset_id) < 0)
            TEST_ERROR
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
 * A test to check that a dataset can be created using
 * string datatypes.
 */
static int
test_create_dataset_string_types(void)
{
    hsize_t dims[DATASET_STRING_TYPE_TEST_SPACE_RANK];
    size_t  i;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t   dset_id_fixed = H5I_INVALID_HID, dset_id_variable = H5I_INVALID_HID;
    hid_t   type_id_fixed = H5I_INVALID_HID, type_id_variable = H5I_INVALID_HID;
    hid_t   fspace_id = H5I_INVALID_HID;

    TESTING("dataset creation with string types")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_STRING_TYPE_TEST_SUBGROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATASET_STRING_TYPE_TEST_SUBGROUP_NAME);
        goto error;
    }

    if ((type_id_fixed = H5Tcreate(H5T_STRING, DATASET_STRING_TYPE_TEST_STRING_LENGTH)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create fixed-length string type\n");
        goto error;
    }

    if ((type_id_variable = H5Tcreate(H5T_STRING, H5T_VARIABLE)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create variable-length string type\n");
        goto error;
    }

    for (i = 0; i < DATASET_STRING_TYPE_TEST_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((fspace_id = H5Screate_simple(DATASET_STRING_TYPE_TEST_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_id_fixed = H5Dcreate2(group_id, DATASET_STRING_TYPE_TEST_DSET_NAME1, type_id_fixed, fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create fixed-length string dataset '%s'\n", DATASET_STRING_TYPE_TEST_DSET_NAME1);
        goto error;
    }

    if ((dset_id_variable = H5Dcreate2(group_id, DATASET_STRING_TYPE_TEST_DSET_NAME2, type_id_variable, fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create variable-length string dataset '%s'\n", DATASET_STRING_TYPE_TEST_DSET_NAME2);
        goto error;
    }

    if (H5Dclose(dset_id_fixed) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id_variable) < 0)
        TEST_ERROR

    if ((dset_id_fixed = H5Dopen2(group_id, DATASET_STRING_TYPE_TEST_DSET_NAME1, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    failed to open dataset '%s'\n", DATASET_STRING_TYPE_TEST_DSET_NAME1);
        goto error;
    }

    if ((dset_id_variable = H5Dopen2(group_id, DATASET_STRING_TYPE_TEST_DSET_NAME2, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    failed to open dataset '%s'\n", DATASET_STRING_TYPE_TEST_DSET_NAME2);
        goto error;
    }

    if (H5Tclose(type_id_fixed) < 0)
        TEST_ERROR
    if (H5Tclose(type_id_variable) < 0)
        TEST_ERROR
    if (H5Sclose(fspace_id) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id_fixed) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id_variable) < 0)
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
        H5Tclose(type_id_fixed);
        H5Tclose(type_id_variable);
        H5Sclose(fspace_id);
        H5Dclose(dset_id_fixed);
        H5Dclose(dset_id_variable);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that a dataset can be created using
 * a variety of compound datatypes.
 */
static int
test_create_dataset_compound_types(void)
{
    hsize_t  dims[DATASET_COMPOUND_TYPE_TEST_DSET_RANK];
    size_t   i, j;
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t    compound_type = H5I_INVALID_HID;
    hid_t    dset_id = H5I_INVALID_HID;
    hid_t    fspace_id = H5I_INVALID_HID;
    hid_t    type_pool[DATASET_COMPOUND_TYPE_TEST_MAX_SUBTYPES];
    int      num_passes;

    TESTING("dataset creation with compound datatypes")

    /*
     * Make sure to pre-initialize all the compound field IDs
     * so we don't try to close an uninitialized ID value;
     * memory checkers will likely complain.
     */
    for (j = 0; j < DATASET_COMPOUND_TYPE_TEST_MAX_SUBTYPES; j++)
        type_pool[j] = H5I_INVALID_HID;

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_COMPOUND_TYPE_TEST_SUBGROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATASET_COMPOUND_TYPE_TEST_SUBGROUP_NAME);
        goto error;
    }

    for (i = 0; i < DATASET_COMPOUND_TYPE_TEST_DSET_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((fspace_id = H5Screate_simple(DATASET_COMPOUND_TYPE_TEST_DSET_RANK, dims, NULL)) < 0)
        TEST_ERROR

    num_passes = (rand() % DATASET_COMPOUND_TYPE_TEST_MAX_PASSES) + 1;

    for (i = 0; i < (size_t) num_passes; i++) {
        size_t num_subtypes;
        size_t compound_size = 0;
        size_t next_offset = 0;
        char   dset_name[256];

        /*
         * Also pre-initialize all of the compound field IDs at the
         * beginning of each loop so that we don't try to close an
         * invalid ID.
         */
        for (j = 0; j < DATASET_COMPOUND_TYPE_TEST_MAX_SUBTYPES; j++)
            type_pool[j] = H5I_INVALID_HID;

        num_subtypes = (size_t) (rand() % DATASET_COMPOUND_TYPE_TEST_MAX_SUBTYPES) + 1;

        if ((compound_type = H5Tcreate(H5T_COMPOUND, 1)) < 0) {
            H5_FAILED();
            HDprintf("    couldn't create compound datatype\n");
            goto error;
        }

        /* Start adding subtypes to the compound type */
        for (j = 0; j < num_subtypes; j++) {
            size_t member_size;
            char   member_name[256];

            HDsnprintf(member_name, 256, "member%zu", j);

            if ((type_pool[j] = generate_random_datatype(H5T_NO_CLASS)) < 0) {
                H5_FAILED();
                HDprintf("    couldn't create compound datatype member %zu\n", j);
                goto error;
            }

            if (!(member_size = H5Tget_size(type_pool[j]))) {
                H5_FAILED();
                HDprintf("    couldn't get compound member %zu size\n", j);
                goto error;
            }

            compound_size += member_size;

            if (H5Tset_size(compound_type, compound_size) < 0)
                TEST_ERROR

            if (H5Tinsert(compound_type, member_name, next_offset, type_pool[j]) < 0)
                TEST_ERROR

            next_offset += member_size;
        }

        if (H5Tpack(compound_type) < 0)
            TEST_ERROR

        HDsnprintf(dset_name, sizeof(dset_name), "%s%zu", DATASET_COMPOUND_TYPE_TEST_DSET_NAME, i);

        if ((dset_id = H5Dcreate2(group_id, dset_name, compound_type, fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDprintf("    couldn't create dataset '%s'\n", dset_name);
            goto error;
        }

        if (H5Dclose(dset_id) < 0)
            TEST_ERROR

        if ((dset_id = H5Dopen2(group_id, dset_name, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDprintf("    failed to open dataset '%s'\n", dset_name);
            goto error;
        }

        for (j = 0; j < num_subtypes; j++)
            if (type_pool[j] >= 0 && H5Tclose(type_pool[j]) < 0)
                TEST_ERROR
        if (H5Tclose(compound_type) < 0)
            TEST_ERROR
        if (H5Dclose(dset_id) < 0)
            TEST_ERROR
    }

    if (H5Sclose(fspace_id) < 0)
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
        for (i = 0; i < DATASET_COMPOUND_TYPE_TEST_MAX_SUBTYPES; i++)
            H5Tclose(type_pool[i]);
        H5Tclose(compound_type);
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
 * A test to check that a dataset can be created with
 * enum datatypes.
 */
static int
test_create_dataset_enum_types(void)
{
    hsize_t     dims[DATASET_ENUM_TYPE_TEST_SPACE_RANK];
    size_t      i;
    hid_t       file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t       container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t       dset_id_native = H5I_INVALID_HID, dset_id_non_native = H5I_INVALID_HID;
    hid_t       fspace_id = H5I_INVALID_HID;
    hid_t       enum_native = H5I_INVALID_HID, enum_non_native = H5I_INVALID_HID;
    const char *enum_type_test_table[] = {
            "RED",    "GREEN",  "BLUE",
            "BLACK",  "WHITE",  "PURPLE",
            "ORANGE", "YELLOW", "BROWN"
    };

    TESTING("dataset creation with enum types")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_ENUM_TYPE_TEST_SUBGROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATASET_ENUM_TYPE_TEST_SUBGROUP_NAME);
        goto error;
    }

    if ((enum_native = H5Tcreate(H5T_ENUM, sizeof(int))) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create native enum type\n");
        goto error;
    }

    for (i = 0; i < ARRAY_LENGTH(enum_type_test_table); i++)
        if (H5Tenum_insert(enum_native, enum_type_test_table[i], &i) < 0)
            TEST_ERROR

    if ((enum_non_native = H5Tenum_create(H5T_STD_U32LE)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create non-native enum type\n");
        goto error;
    }

    for (i = 0; i < DATASET_ENUM_TYPE_TEST_NUM_MEMBERS; i++) {
        char val_name[15];

        HDsprintf(val_name, "%s%zu", DATASET_ENUM_TYPE_TEST_VAL_BASE_NAME, i);

        if (H5Tenum_insert(enum_non_native, val_name, &i) < 0)
            TEST_ERROR
    }

    for (i = 0; i < DATASET_ENUM_TYPE_TEST_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((fspace_id = H5Screate_simple(DATASET_ENUM_TYPE_TEST_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_id_native = H5Dcreate2(group_id, DATASET_ENUM_TYPE_TEST_DSET_NAME1, enum_native, fspace_id,
            H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create native enum dataset '%s'\n", DATASET_ENUM_TYPE_TEST_DSET_NAME1);
        goto error;
    }

    if ((dset_id_non_native = H5Dcreate2(group_id, DATASET_ENUM_TYPE_TEST_DSET_NAME2, enum_non_native, fspace_id,
            H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create non-native enum dataset '%s'\n", DATASET_ENUM_TYPE_TEST_DSET_NAME2);
        goto error;
    }

    if (H5Dclose(dset_id_native) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id_non_native) < 0)
        TEST_ERROR

    if ((dset_id_native = H5Dopen2(group_id, DATASET_ENUM_TYPE_TEST_DSET_NAME1, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    failed to open dataset '%s'\n", DATASET_ENUM_TYPE_TEST_DSET_NAME1);
        goto error;
    }

    if ((dset_id_non_native = H5Dopen2(group_id, DATASET_ENUM_TYPE_TEST_DSET_NAME2, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    failed to open dataset '%s'\n", DATASET_ENUM_TYPE_TEST_DSET_NAME2);
        goto error;
    }

    if (H5Tclose(enum_native) < 0)
        TEST_ERROR
    if (H5Tclose(enum_non_native) < 0)
        TEST_ERROR
    if (H5Sclose(fspace_id) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id_native) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id_non_native) < 0)
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
        H5Tclose(enum_native);
        H5Tclose(enum_non_native);
        H5Sclose(fspace_id);
        H5Dclose(dset_id_native);
        H5Dclose(dset_id_non_native);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that a dataset can be created using
 * array datatypes.
 */
static int
test_create_dataset_array_types(void)
{
    hsize_t dset_dims[DATASET_ARRAY_TYPE_TEST_SPACE_RANK];
    hsize_t array_dims1[DATASET_ARRAY_TYPE_TEST_RANK1];
    hsize_t array_dims2[DATASET_ARRAY_TYPE_TEST_RANK2];
    hsize_t array_dims3[DATASET_ARRAY_TYPE_TEST_RANK3];
    size_t  i;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t   dset_id1 = H5I_INVALID_HID, dset_id2 = H5I_INVALID_HID, dset_id3 = H5I_INVALID_HID;
    hid_t   fspace_id = H5I_INVALID_HID;
    hid_t   array_type_id1 = H5I_INVALID_HID, array_type_id2 = H5I_INVALID_HID, array_type_id3 = H5I_INVALID_HID;
    hid_t   array_base_type_id1 = H5I_INVALID_HID, array_base_type_id2 = H5I_INVALID_HID, array_base_type_id3 = H5I_INVALID_HID;
    hid_t   nested_type_id = H5I_INVALID_HID;

    TESTING("dataset creation with array types")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_ARRAY_TYPE_TEST_SUBGROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATASET_ARRAY_TYPE_TEST_SUBGROUP_NAME);
        goto error;
    }

    /* Test creation of array with some different types */
    for (i = 0; i < DATASET_ARRAY_TYPE_TEST_RANK1; i++)
        array_dims1[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((array_base_type_id1 = generate_random_datatype(H5T_ARRAY)) < 0)
        TEST_ERROR

    if ((array_type_id1 = H5Tarray_create2(array_base_type_id1, DATASET_ARRAY_TYPE_TEST_RANK1, array_dims1)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create first array type\n");
        goto error;
    }

    for (i = 0; i < DATASET_ARRAY_TYPE_TEST_RANK2; i++)
        array_dims2[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((array_base_type_id2 = generate_random_datatype(H5T_ARRAY)) < 0)
        TEST_ERROR

    if ((array_type_id2 = H5Tarray_create2(array_base_type_id2, DATASET_ARRAY_TYPE_TEST_RANK2, array_dims2)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create second array type\n");
        goto error;
    }

    /* Test nested arrays */
    for (i = 0; i < DATASET_ARRAY_TYPE_TEST_RANK3; i++)
        array_dims3[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((array_base_type_id3 = generate_random_datatype(H5T_ARRAY)) < 0)
        TEST_ERROR

    if ((nested_type_id = H5Tarray_create2(array_base_type_id3, DATASET_ARRAY_TYPE_TEST_RANK3, array_dims3)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create nested array base type\n");
        goto error;
    }

    if ((array_type_id3 = H5Tarray_create2(nested_type_id, DATASET_ARRAY_TYPE_TEST_RANK3, array_dims3)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create nested array type\n");
        goto error;
    }

    for (i = 0; i < DATASET_ARRAY_TYPE_TEST_SPACE_RANK; i++)
        dset_dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((fspace_id = H5Screate_simple(DATASET_ARRAY_TYPE_TEST_SPACE_RANK, dset_dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_id1 = H5Dcreate2(group_id, DATASET_ARRAY_TYPE_TEST_DSET_NAME1, array_type_id1, fspace_id,
            H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create array type dataset '%s'\n", DATASET_ARRAY_TYPE_TEST_DSET_NAME1);
        goto error;
    }

    if ((dset_id2 = H5Dcreate2(group_id, DATASET_ARRAY_TYPE_TEST_DSET_NAME2, array_type_id2, fspace_id,
            H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create array type dataset '%s'\n", DATASET_ARRAY_TYPE_TEST_DSET_NAME2);
        goto error;
    }

    if ((dset_id3 = H5Dcreate2(group_id, DATASET_ARRAY_TYPE_TEST_DSET_NAME3, array_type_id3, fspace_id,
            H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create nested array type dataset '%s'\n", DATASET_ARRAY_TYPE_TEST_DSET_NAME3);
        goto error;
    }

    if (H5Dclose(dset_id1) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id2) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id3) < 0)
        TEST_ERROR

    if ((dset_id1 = H5Dopen2(group_id, DATASET_ARRAY_TYPE_TEST_DSET_NAME1, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    failed to open dataset '%s'\n", DATASET_ARRAY_TYPE_TEST_DSET_NAME1);
        goto error;
    }

    if ((dset_id2 = H5Dopen2(group_id, DATASET_ARRAY_TYPE_TEST_DSET_NAME2, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    failed to open dataset '%s'\n", DATASET_ARRAY_TYPE_TEST_DSET_NAME2);
        goto error;
    }

    if ((dset_id3 = H5Dopen2(group_id, DATASET_ARRAY_TYPE_TEST_DSET_NAME3, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    failed to open dataset '%s'\n", DATASET_ARRAY_TYPE_TEST_DSET_NAME3);
        goto error;
    }

    if (H5Tclose(array_base_type_id1) < 0)
        TEST_ERROR
    if (H5Tclose(array_base_type_id2) < 0)
        TEST_ERROR
    if (H5Tclose(array_base_type_id3) < 0)
        TEST_ERROR
    if (H5Tclose(nested_type_id) < 0)
        TEST_ERROR
    if (H5Tclose(array_type_id1) < 0)
        TEST_ERROR
    if (H5Tclose(array_type_id2) < 0)
        TEST_ERROR
    if (H5Tclose(array_type_id3) < 0)
        TEST_ERROR
    if (H5Sclose(fspace_id) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id1) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id2) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id3) < 0)
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
        H5Tclose(array_base_type_id1);
        H5Tclose(array_base_type_id2);
        H5Tclose(array_base_type_id3);
        H5Tclose(nested_type_id);
        H5Tclose(array_type_id1);
        H5Tclose(array_type_id2);
        H5Tclose(array_type_id3);
        H5Sclose(fspace_id);
        H5Dclose(dset_id1);
        H5Dclose(dset_id2);
        H5Dclose(dset_id3);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check the functionality of the different
 * dataset creation properties.
 */
static int
test_create_dataset_creation_properties(void)
{
    hsize_t dims[DATASET_CREATION_PROPERTIES_TEST_SHAPE_RANK];
    size_t  i;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t   dset_id = H5I_INVALID_HID, dcpl_id = H5I_INVALID_HID;
    hid_t   dset_dtype = H5I_INVALID_HID;
    hid_t   fspace_id = H5I_INVALID_HID;

    TESTING("dataset creation properties")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_CREATION_PROPERTIES_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create group '%s'\n", DATASET_CREATION_PROPERTIES_TEST_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < DATASET_CREATION_PROPERTIES_TEST_SHAPE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((fspace_id = H5Screate_simple(DATASET_CREATION_PROPERTIES_TEST_SHAPE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    /* Test the alloc time property */
    {
        H5D_alloc_time_t alloc_times[] = {
                H5D_ALLOC_TIME_DEFAULT, H5D_ALLOC_TIME_EARLY,
                H5D_ALLOC_TIME_INCR, H5D_ALLOC_TIME_LATE
        };

        if ((dcpl_id = H5Pcreate(H5P_DATASET_CREATE)) < 0)
            TEST_ERROR

        for (i = 0; i < ARRAY_LENGTH(alloc_times); i++) {
            char name[100];

            if (H5Pset_alloc_time(dcpl_id, alloc_times[i]) < 0)
                TEST_ERROR

            HDsprintf(name, "%s%zu", DATASET_CREATION_PROPERTIES_TEST_ALLOC_TIMES_BASE_NAME, i);

            if ((dset_id = H5Dcreate2(group_id, name, dset_dtype, fspace_id, H5P_DEFAULT, dcpl_id, H5P_DEFAULT)) < 0) {
                H5_FAILED();
                HDprintf("    couldn't create dataset '%s'\n", name);
                goto error;
            }

            if (H5Dclose(dset_id) < 0)
                TEST_ERROR

            if ((dset_id = H5Dopen2(group_id, name, H5P_DEFAULT)) < 0) {
                H5_FAILED();
                HDprintf("    couldn't open dataset '%s'\n", name);
                goto error;
            }

            if (H5Dclose(dset_id) < 0)
                TEST_ERROR
        }

        if (H5Pclose(dcpl_id) < 0)
            TEST_ERROR
    }

    /* Test the attribute creation order property */
    {
        unsigned creation_orders[] = {
                H5P_CRT_ORDER_TRACKED,
                H5P_CRT_ORDER_TRACKED | H5P_CRT_ORDER_INDEXED
        };

        if ((dcpl_id = H5Pcreate(H5P_DATASET_CREATE)) < 0)
            TEST_ERROR

        for (i = 0; i < ARRAY_LENGTH(creation_orders); i++) {
            char name[100];

            if (H5Pset_attr_creation_order(dcpl_id, creation_orders[i]) < 0)
                TEST_ERROR

            HDsprintf(name, "%s%zu", DATASET_CREATION_PROPERTIES_TEST_CRT_ORDER_BASE_NAME, i);

            if ((dset_id = H5Dcreate2(group_id, name, dset_dtype, fspace_id, H5P_DEFAULT, dcpl_id, H5P_DEFAULT)) < 0) {
                H5_FAILED();
                HDprintf("    couldn't create dataset '%s'\n", name);
                goto error;
            }

            if (H5Dclose(dset_id) < 0)
                TEST_ERROR

            if ((dset_id = H5Dopen2(group_id, name, H5P_DEFAULT)) < 0) {
                H5_FAILED();
                HDprintf("    couldn't open dataset '%s'\n", name);
                goto error;
            }

            if (H5Dclose(dset_id) < 0)
                TEST_ERROR
        }

        if (H5Pclose(dcpl_id) < 0)
            TEST_ERROR
    }

    /* Test the attribute phase change property */
    {
        if ((dcpl_id = H5Pcreate(H5P_DATASET_CREATE)) < 0)
            TEST_ERROR

        if (H5Pset_attr_phase_change(dcpl_id, DATASET_CREATION_PROPERTIES_TEST_MAX_COMPACT,
                DATASET_CREATION_PROPERTIES_TEST_MIN_DENSE) < 0)
            TEST_ERROR

        if ((dset_id = H5Dcreate2(group_id, DATASET_CREATION_PROPERTIES_TEST_PHASE_CHANGE_DSET_NAME,
                dset_dtype, fspace_id, H5P_DEFAULT, dcpl_id, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDprintf("    couldn't create dataset '%s'\n", DATASET_CREATION_PROPERTIES_TEST_PHASE_CHANGE_DSET_NAME);
            goto error;
        }

        if (H5Dclose(dset_id) < 0)
            TEST_ERROR

        if ((dset_id = H5Dopen2(group_id, DATASET_CREATION_PROPERTIES_TEST_PHASE_CHANGE_DSET_NAME, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDprintf("    couldn't open dataset '%s'\n", DATASET_CREATION_PROPERTIES_TEST_PHASE_CHANGE_DSET_NAME);
            goto error;
        }

        if (H5Dclose(dset_id) < 0)
            TEST_ERROR
        if (H5Pclose(dcpl_id) < 0)
            TEST_ERROR
    }

    /* Test the fill time property */
    {
        H5D_fill_time_t fill_times[] = {
                H5D_FILL_TIME_IFSET, H5D_FILL_TIME_ALLOC,
                H5D_FILL_TIME_NEVER
        };

        if ((dcpl_id = H5Pcreate(H5P_DATASET_CREATE)) < 0)
            TEST_ERROR

        for (i = 0; i < ARRAY_LENGTH(fill_times); i++) {
            char name[100];

            if (H5Pset_fill_time(dcpl_id, fill_times[i]) < 0)
                TEST_ERROR

            HDsprintf(name, "%s%zu", DATASET_CREATION_PROPERTIES_TEST_FILL_TIMES_BASE_NAME, i);

            if ((dset_id = H5Dcreate2(group_id, name, dset_dtype, fspace_id, H5P_DEFAULT, dcpl_id, H5P_DEFAULT)) < 0) {
                H5_FAILED();
                HDprintf("    couldn't create dataset '%s'\n", name);
                goto error;
            }

            if (H5Dclose(dset_id) < 0)
                TEST_ERROR

            if ((dset_id = H5Dopen2(group_id, name, H5P_DEFAULT)) < 0) {
                H5_FAILED();
                HDprintf("    couldn't open dataset '%s'\n", name);
                goto error;
            }

            if (H5Dclose(dset_id) < 0)
                TEST_ERROR
        }

        if (H5Pclose(dcpl_id) < 0)
            TEST_ERROR
    }

    /* TODO: Test the fill value property */
    {

    }

    {
        if ((dcpl_id = H5Pcreate(H5P_DATASET_CREATE)) < 0)
            TEST_ERROR

        /* Set all of the available filters on the DCPL */
        if (H5Pset_deflate(dcpl_id, 7) < 0)
            TEST_ERROR
        if (H5Pset_shuffle(dcpl_id) < 0)
            TEST_ERROR
        if (H5Pset_fletcher32(dcpl_id) < 0)
            TEST_ERROR
        if (H5Pset_nbit(dcpl_id) < 0)
            TEST_ERROR
        if (H5Pset_scaleoffset(dcpl_id, H5Z_SO_FLOAT_ESCALE, 2) < 0)
            TEST_ERROR

        if ((dset_id = H5Dcreate2(group_id, DATASET_CREATION_PROPERTIES_TEST_FILTERS_DSET_NAME, dset_dtype, fspace_id, H5P_DEFAULT, dcpl_id, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDprintf("    couldn't create dataset '%s'\n", DATASET_CREATION_PROPERTIES_TEST_FILTERS_DSET_NAME);
            goto error;
        }

        if (H5Dclose(dset_id) < 0)
            TEST_ERROR

        if ((dset_id = H5Dopen2(group_id, DATASET_CREATION_PROPERTIES_TEST_FILTERS_DSET_NAME, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDprintf("    couldn't open dataset '%s'\n", DATASET_CREATION_PROPERTIES_TEST_FILTERS_DSET_NAME);
            goto error;
        }

        if (H5Dclose(dset_id) < 0)
            TEST_ERROR
        if (H5Pclose(dcpl_id) < 0)
            TEST_ERROR
    }

    /* Test the storage layout property */
    {
        H5D_layout_t layouts[] = {
                H5D_COMPACT, H5D_CONTIGUOUS, H5D_CHUNKED
        };

        if ((dcpl_id = H5Pcreate(H5P_DATASET_CREATE)) < 0)
            TEST_ERROR

        for (i = 0; i < ARRAY_LENGTH(layouts); i++) {
            char name[100];

            if (H5Pset_layout(dcpl_id, layouts[i]) < 0)
                TEST_ERROR

            if (H5D_CHUNKED == layouts[i]) {
                hsize_t chunk_dims[DATASET_CREATION_PROPERTIES_TEST_CHUNK_DIM_RANK];
                size_t  j;

                for (j = 0; j < DATASET_CREATION_PROPERTIES_TEST_CHUNK_DIM_RANK; j++)
                    chunk_dims[j] = (hsize_t) (rand() % (int) dims[j] + 1);

                if (H5Pset_chunk(dcpl_id , DATASET_CREATION_PROPERTIES_TEST_CHUNK_DIM_RANK, chunk_dims) < 0)
                    TEST_ERROR
            }

            HDsprintf(name, "%s%zu", DATASET_CREATION_PROPERTIES_TEST_LAYOUTS_BASE_NAME, i);

            if ((dset_id = H5Dcreate2(group_id, name, dset_dtype, fspace_id, H5P_DEFAULT, dcpl_id, H5P_DEFAULT)) < 0) {
                H5_FAILED();
                HDprintf("    couldn't create dataset '%s'\n", name);
                goto error;
            }

            if (H5Dclose(dset_id) < 0)
                TEST_ERROR

            if ((dset_id = H5Dopen2(group_id, name, H5P_DEFAULT)) < 0) {
                H5_FAILED();
                HDprintf("    couldn't open dataset '%s'\n", name);
                goto error;
            }

            if (H5Dclose(dset_id) < 0)
                TEST_ERROR
        }

        if (H5Pclose(dcpl_id) < 0)
            TEST_ERROR
    }

    /* Test the "track object times" property */
    {
        if ((dcpl_id = H5Pcreate(H5P_DATASET_CREATE)) < 0)
            TEST_ERROR

        if (H5Pset_obj_track_times(dcpl_id, true) < 0)
            TEST_ERROR

        if ((dset_id = H5Dcreate2(group_id, DATASET_CREATION_PROPERTIES_TEST_TRACK_TIMES_YES_DSET_NAME,
                dset_dtype, fspace_id, H5P_DEFAULT, dcpl_id, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDprintf("    couldn't create dataset '%s'\n", DATASET_CREATION_PROPERTIES_TEST_TRACK_TIMES_YES_DSET_NAME);
            goto error;
        }

        if (H5Dclose(dset_id) < 0)
            TEST_ERROR

        if ((dset_id = H5Dopen2(group_id, DATASET_CREATION_PROPERTIES_TEST_TRACK_TIMES_YES_DSET_NAME, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDprintf("    couldn't open dataset '%s'\n", DATASET_CREATION_PROPERTIES_TEST_TRACK_TIMES_YES_DSET_NAME);
            goto error;
        }

        if (H5Dclose(dset_id) < 0)
            TEST_ERROR

        if (H5Pset_obj_track_times(dcpl_id, false) < 0)
            TEST_ERROR

        if ((dset_id = H5Dcreate2(group_id, DATASET_CREATION_PROPERTIES_TEST_TRACK_TIMES_NO_DSET_NAME,
                dset_dtype, fspace_id, H5P_DEFAULT, dcpl_id, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDprintf("    couldn't create dataset '%s'\n", DATASET_CREATION_PROPERTIES_TEST_TRACK_TIMES_NO_DSET_NAME);
            goto error;
        }

        if (H5Dclose(dset_id) < 0)
            TEST_ERROR

        if ((dset_id = H5Dopen2(group_id, DATASET_CREATION_PROPERTIES_TEST_TRACK_TIMES_NO_DSET_NAME, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDprintf("    couldn't open dataset '%s'\n", DATASET_CREATION_PROPERTIES_TEST_TRACK_TIMES_NO_DSET_NAME);
            goto error;
        }

        if (H5Dclose(dset_id) < 0)
            TEST_ERROR
        if (H5Pclose(dcpl_id) < 0)
            TEST_ERROR
    }

    if (H5Sclose(fspace_id) < 0)
        TEST_ERROR
    if (H5Tclose(dset_dtype) < 0)
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
        H5Pclose(dcpl_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to create many small datasets (100,000)
 */
static int
test_create_many_dataset(void)
{
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t   dset_id = H5I_INVALID_HID;
    hid_t   dataspace_id = H5I_INVALID_HID;
    char    dset_name[DSET_NAME_BUF_SIZE];
    unsigned char    data;
    unsigned int     i;

    TESTING("creating many datasets")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_MANY_CREATE_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", DATASET_MANY_CREATE_GROUP_NAME);
        goto error;
    }

    if ((dataspace_id = H5Screate(H5S_SCALAR)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create scalar data space\n");
        goto error;
    }

    for (i = 0; i < DATASET_NUMB; i++) {
        sprintf(dset_name, "dset_%02u", i);
        data = i % 256;

        if ((dset_id = H5Dcreate2(group_id, dset_name, H5T_NATIVE_UCHAR, dataspace_id,
                H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDprintf("    couldn't create dataset '%s'\n", dset_name);
            goto error;
        }

        if (H5Dwrite(dset_id, H5T_NATIVE_UCHAR, H5S_ALL, H5S_ALL, H5P_DEFAULT, &data) < 0) {
            H5_FAILED();
            HDprintf("    couldn't write to dataset '%s'\n", dset_name);
            goto error;
        }

        if (H5Dclose(dset_id) < 0) {
            H5_FAILED();
            HDprintf("    couldn't close dataset '%s'\n", dset_name);
            goto error;
        }
    }

    if (H5Sclose(dataspace_id) < 0)
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
        H5Dclose(dset_id);
        H5Sclose(dataspace_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that re-opening a dataset with
 * H5Dopen succeeds.
 */
static int
test_open_dataset(void)
{
    TESTING("H5Dopen")

    SKIPPED();

    return 0;
}

/*
 * A test to check that H5Dopen fails when it is
 * passed invalid parameters.
 */
static int
test_open_dataset_invalid_params(void)
{
    hsize_t dims[DATASET_OPEN_INVALID_PARAMS_SPACE_RANK];
    size_t  i;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t   dset_id = H5I_INVALID_HID;
    hid_t   dset_dtype = H5I_INVALID_HID;
    hid_t   fspace_id = H5I_INVALID_HID;

    TESTING("H5Dopen with invalid parameters"); HDputs("");

    TESTING_2("H5Dopen with an invalid loc_id")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_OPEN_INVALID_PARAMS_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", DATASET_OPEN_INVALID_PARAMS_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < DATASET_OPEN_INVALID_PARAMS_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((fspace_id = H5Screate_simple(DATASET_OPEN_INVALID_PARAMS_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, DATASET_OPEN_INVALID_PARAMS_DSET_NAME, dset_dtype, fspace_id,
            H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_OPEN_INVALID_PARAMS_DSET_NAME);
        goto error;
    }

    if (H5Dclose(dset_id) < 0)
        TEST_ERROR

    H5E_BEGIN_TRY {
        dset_id = H5Dopen2(H5I_INVALID_HID, DATASET_OPEN_INVALID_PARAMS_DSET_NAME, H5P_DEFAULT);
    } H5E_END_TRY;

    if (dset_id >= 0) {
        H5_FAILED();
        HDprintf("    opened dataset using H5Dopen2 with an invalid loc_id!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dopen with an invalid dataset name")

    H5E_BEGIN_TRY {
        dset_id = H5Dopen2(group_id, NULL, H5P_DEFAULT);
    } H5E_END_TRY;

    if (dset_id >= 0) {
        H5_FAILED();
        HDprintf("    opened dataset using H5Dopen2 with an invalid dataset name!\n");
        goto error;
    }

    H5E_BEGIN_TRY {
        dset_id = H5Dopen2(group_id, "", H5P_DEFAULT);
    } H5E_END_TRY;

    if (dset_id >= 0) {
        H5_FAILED();
        HDprintf("    opened dataset using H5Dopen2 with an invalid dataset name!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dopen with an invalid DAPL")

    H5E_BEGIN_TRY {
        dset_id = H5Dopen2(group_id, DATASET_OPEN_INVALID_PARAMS_DSET_NAME, H5I_INVALID_HID);
    } H5E_END_TRY;

    if (dset_id >= 0) {
        H5_FAILED();
        HDprintf("    opened dataset using H5Dopen2 with an invalid DAPL!\n");
        goto error;
    }

    if (H5Sclose(fspace_id) < 0)
        TEST_ERROR
    if (H5Tclose(dset_dtype) < 0)
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
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that H5Dclose fails when it is
 * passed an invalid dataset ID.
 */
static int
test_close_dataset_invalid_params(void)
{
    herr_t err_ret = -1;
    hid_t  file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;

    TESTING("H5Dclose with an invalid dataset ID")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Dclose(H5I_INVALID_HID);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    H5Dclose succeeded with an invalid dataset ID!\n");
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
 * A test to check that valid copies of a dataset's dataspace
 * and datatype can be retrieved with H5Dget_space and
 * H5Dget_type, respectively.
 */
static int
test_get_dataset_space_and_type(void)
{
    hsize_t dset_dims[DATASET_GET_SPACE_TYPE_TEST_SPACE_RANK];
    size_t  i;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID;
    hid_t   group_id = H5I_INVALID_HID;
    hid_t   dset_id = H5I_INVALID_HID;
    hid_t   dset_dtype = H5I_INVALID_HID;
    hid_t   dset_space_id = H5I_INVALID_HID;
    hid_t   tmp_type_id = H5I_INVALID_HID;
    hid_t   tmp_space_id = H5I_INVALID_HID;

    TESTING("retrieval of a dataset's dataspace and datatype"); HDputs("");

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_GET_SPACE_TYPE_TEST_GROUP_NAME, H5P_DEFAULT,  H5P_DEFAULT,  H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", DATASET_GET_SPACE_TYPE_TEST_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < DATASET_GET_SPACE_TYPE_TEST_SPACE_RANK; i++)
        dset_dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((dset_space_id = H5Screate_simple(DATASET_GET_SPACE_TYPE_TEST_SPACE_RANK, dset_dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, DATASET_GET_SPACE_TYPE_TEST_DSET_NAME, dset_dtype,
            dset_space_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_GET_SPACE_TYPE_TEST_DSET_NAME);
        goto error;
    }

    /* Retrieve the dataset's datatype and dataspace and verify them */
    TESTING_2("H5Dget_type")

    if ((tmp_type_id = H5Dget_type(dset_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve dataset's datatype\n");
        goto error;
    }

    {
        htri_t types_equal = H5Tequal(tmp_type_id, dset_dtype);

        if (types_equal < 0) {
            H5_FAILED();
            HDprintf("    datatype was invalid\n");
            goto error;
        }

        if (!types_equal) {
            H5_FAILED();
            HDprintf("    dataset's datatype did not match\n");
            goto error;
        }
    }

    PASSED();

    TESTING_2("H5Dget_space")

    if ((tmp_space_id = H5Dget_space(dset_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve dataset's dataspace\n");
        goto error;
    }

    {
        hsize_t space_dims[DATASET_GET_SPACE_TYPE_TEST_SPACE_RANK];

        if (H5Sget_simple_extent_dims(tmp_space_id, space_dims, NULL) < 0)
            TEST_ERROR

        for (i = 0; i < DATASET_GET_SPACE_TYPE_TEST_SPACE_RANK; i++)
            if (space_dims[i] != dset_dims[i]) {
                H5_FAILED();
                HDprintf("    dataset's dataspace dims didn't match\n");
                goto error;
            }
    }

    PASSED();

    TESTING_2("H5Dget_type after re-opening a dataset")

    /* Now close the dataset and verify that this still works after
     * opening an attribute instead of creating it.
     */
    if (H5Dclose(dset_id) < 0)
        TEST_ERROR
    if (H5Tclose(tmp_type_id) < 0)
        TEST_ERROR
    if (H5Sclose(tmp_space_id) < 0)
        TEST_ERROR

    if ((dset_id = H5Dopen2(group_id, DATASET_GET_SPACE_TYPE_TEST_DSET_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open dataset '%s'\n", DATASET_GET_SPACE_TYPE_TEST_DSET_NAME);
        goto error;
    }

    if ((tmp_type_id = H5Dget_type(dset_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve dataset's datatype\n");
        goto error;
    }

    {
        htri_t types_equal = H5Tequal(tmp_type_id, dset_dtype);

        if (types_equal < 0) {
            H5_FAILED();
            HDprintf("    datatype was invalid\n");
            goto error;
        }

        if (!types_equal) {
            H5_FAILED();
            HDprintf("    dataset's datatype did not match\n");
            goto error;
        }
    }

    PASSED();

    TESTING_2("H5Dget_space after re-opening a dataset")

    if ((tmp_space_id = H5Dget_space(dset_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't retrieve dataset's dataspace\n");
        goto error;
    }

    {
        hsize_t space_dims[DATASET_GET_SPACE_TYPE_TEST_SPACE_RANK];

        if (H5Sget_simple_extent_dims(tmp_space_id, space_dims, NULL) < 0)
            TEST_ERROR

        for (i = 0; i < DATASET_GET_SPACE_TYPE_TEST_SPACE_RANK; i++) {
            if (space_dims[i] != dset_dims[i]) {
                H5_FAILED();
                HDprintf("    dataset's dataspace dims didn't match!\n");
                goto error;
            }
        }
    }

    if (H5Sclose(tmp_space_id) < 0)
        TEST_ERROR
    if (H5Sclose(dset_space_id) < 0)
        TEST_ERROR
    if (H5Tclose(tmp_type_id) < 0)
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
        H5Sclose(tmp_space_id);
        H5Sclose(dset_space_id);
        H5Tclose(tmp_type_id);
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
 * A test to check that a dataset's dataspace and datatype
 * can't be retrieved when H5Dget_space and H5Dget_type are passed
 * invalid parameters, respectively.
 */
static int
test_get_dataset_space_and_type_invalid_params(void)
{
    hsize_t dset_dims[DATASET_GET_SPACE_TYPE_INVALID_PARAMS_TEST_SPACE_RANK];
    size_t  i;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID;
    hid_t   group_id = H5I_INVALID_HID;
    hid_t   dset_id = H5I_INVALID_HID;
    hid_t   dset_dtype = H5I_INVALID_HID;
    hid_t   dset_space_id = H5I_INVALID_HID;
    hid_t   tmp_type_id = H5I_INVALID_HID;
    hid_t   tmp_space_id = H5I_INVALID_HID;

    TESTING("H5Dget_type/H5Dget_space with invalid parameters"); HDputs("");

    TESTING_2("H5Dget_type with an invalid attr_id")

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

    if ((group_id = H5Gcreate2(container_group, DATASET_GET_SPACE_TYPE_INVALID_PARAMS_TEST_GROUP_NAME, H5P_DEFAULT,  H5P_DEFAULT,  H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container group '%s'\n", DATASET_GET_SPACE_TYPE_INVALID_PARAMS_TEST_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < DATASET_GET_SPACE_TYPE_INVALID_PARAMS_TEST_SPACE_RANK; i++)
        dset_dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);

    if ((dset_space_id = H5Screate_simple(DATASET_GET_SPACE_TYPE_INVALID_PARAMS_TEST_SPACE_RANK, dset_dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, DATASET_GET_SPACE_TYPE_INVALID_PARAMS_TEST_DSET_NAME, dset_dtype,
            dset_space_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_GET_SPACE_TYPE_INVALID_PARAMS_TEST_DSET_NAME);
        goto error;
    }

    H5E_BEGIN_TRY {
        tmp_type_id = H5Dget_type(H5I_INVALID_HID);
    } H5E_END_TRY;

    if (tmp_type_id >= 0) {
        H5_FAILED();
        HDprintf("    retrieved copy of dataset's datatype using an invalid dataset ID!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dget_space with an invalid attr_id")

    H5E_BEGIN_TRY {
        tmp_space_id = H5Dget_space(H5I_INVALID_HID);
    } H5E_END_TRY;

    if (tmp_space_id >= 0) {
        H5_FAILED();
        HDprintf("    retrieved copy of dataset's dataspace using an invalid dataset ID!\n");
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
        H5Sclose(tmp_space_id);
        H5Sclose(dset_space_id);
        H5Tclose(tmp_type_id);
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
 * A test for H5Dget_space_status.
 */
static int
test_get_dataset_space_status(void)
{
    TESTING("H5Dget_space_status")

    SKIPPED();

    return 0;
}

/*
 * A test to check that a dataset's dataspace allocation
 * status can't be retrieved with H5Dget_space_status when
 * it is passed invalid parameters.
 */
static int
test_get_dataset_space_status_invalid_params(void)
{
    TESTING("H5Dget_space_status with invalid parameters")

    SKIPPED();

    return 0;
}

/*
 * A test to check that a VOL connector stores and can
 * retrieve valid copies of a DAPL and DCPL used at
 * dataset access and dataset creation, respectively.
 */
static int
test_dataset_property_lists(void)
{
    const char *path_prefix = "/test_prefix";
    hsize_t     dims[DATASET_PROPERTY_LIST_TEST_SPACE_RANK];
    hsize_t     chunk_dims[DATASET_PROPERTY_LIST_TEST_SPACE_RANK];
    size_t      i;
    herr_t      err_ret = -1;
    hid_t       file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t       container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t       dset_id1 = H5I_INVALID_HID, dset_id2 = H5I_INVALID_HID, dset_id3 = H5I_INVALID_HID, dset_id4 = H5I_INVALID_HID;
    hid_t       dcpl_id1 = H5I_INVALID_HID, dcpl_id2 = H5I_INVALID_HID;
    hid_t       dapl_id1 = H5I_INVALID_HID, dapl_id2 = H5I_INVALID_HID;
    hid_t       dset_dtype1 = H5I_INVALID_HID, dset_dtype2 = H5I_INVALID_HID, dset_dtype3 = H5I_INVALID_HID, dset_dtype4 = H5I_INVALID_HID;
    hid_t       space_id = H5I_INVALID_HID;
    char       *tmp_prefix = NULL;

    TESTING("dataset property list operations"); HDputs("");

    TESTING_2("H5Dget_create_plist")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_PROPERTY_LIST_TEST_SUBGROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATASET_PROPERTY_LIST_TEST_SUBGROUP_NAME);
        goto error;
    }

    for (i = 0; i < DATASET_PROPERTY_LIST_TEST_SPACE_RANK; i++)
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);
    for (i = 0; i < DATASET_PROPERTY_LIST_TEST_SPACE_RANK; i++)
        chunk_dims[i] = (hsize_t) (rand() % (int) dims[i] + 1);

    if ((space_id = H5Screate_simple(DATASET_PROPERTY_LIST_TEST_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_dtype1 = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR
    if ((dset_dtype2 = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR
    if ((dset_dtype3 = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR
    if ((dset_dtype4 = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((dcpl_id1 = H5Pcreate(H5P_DATASET_CREATE)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create DCPL\n");
        goto error;
    }

    if (H5Pset_chunk(dcpl_id1, DATASET_PROPERTY_LIST_TEST_SPACE_RANK, chunk_dims) < 0) {
        H5_FAILED();
        HDprintf("    couldn't set DCPL property\n");
        goto error;
    }

    if ((dset_id1 = H5Dcreate2(group_id, DATASET_PROPERTY_LIST_TEST_DSET_NAME1, dset_dtype1,
            space_id, H5P_DEFAULT, dcpl_id1, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_PROPERTY_LIST_TEST_DSET_NAME1);
        goto error;
    }

    if ((dset_id2 = H5Dcreate2(group_id, DATASET_PROPERTY_LIST_TEST_DSET_NAME2, dset_dtype2,
            space_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_PROPERTY_LIST_TEST_DSET_NAME2);
        goto error;
    }

    if (H5Pclose(dcpl_id1) < 0)
        TEST_ERROR

    /* Try to receive copies of the two property lists, one which has the property set and one which does not */
    if ((dcpl_id1 = H5Dget_create_plist(dset_id1)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get property list\n");
        goto error;
    }

    if ((dcpl_id2 = H5Dget_create_plist(dset_id2)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get property list\n");
        goto error;
    }

    /* Ensure that property list 1 has the property set and property list 2 does not */
    {
        hsize_t tmp_chunk_dims[DATASET_PROPERTY_LIST_TEST_SPACE_RANK];

        HDmemset(tmp_chunk_dims, 0, sizeof(tmp_chunk_dims));

        if (H5Pget_chunk(dcpl_id1, DATASET_PROPERTY_LIST_TEST_SPACE_RANK, tmp_chunk_dims) < 0) {
            H5_FAILED();
            HDprintf("    couldn't get DCPL property value\n");
            goto error;
        }

        for (i = 0; i < DATASET_PROPERTY_LIST_TEST_SPACE_RANK; i++)
            if (tmp_chunk_dims[i] != chunk_dims[i]) {
                H5_FAILED();
                HDprintf("    DCPL property values were incorrect\n");
                goto error;
            }

        H5E_BEGIN_TRY {
            err_ret = H5Pget_chunk(dcpl_id2, DATASET_PROPERTY_LIST_TEST_SPACE_RANK, tmp_chunk_dims);
        } H5E_END_TRY;

        if (err_ret >= 0) {
            H5_FAILED();
            HDprintf("    property list 2 shouldn't have had chunk dimensionality set (not a chunked layout)\n");
            goto error;
        }
    }

    PASSED();

    TESTING_2("H5Dget_access_plist")

    if ((dapl_id1 = H5Pcreate(H5P_DATASET_ACCESS)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create DAPL\n");
        goto error;
    }

    if (H5Pset_efile_prefix(dapl_id1, path_prefix) < 0) {
        H5_FAILED();
        HDprintf("    couldn't set DAPL property\n");
        goto error;
    }

    if ((dset_id3 = H5Dcreate2(group_id, DATASET_PROPERTY_LIST_TEST_DSET_NAME3, dset_dtype3,
            space_id, H5P_DEFAULT, H5P_DEFAULT, dapl_id1)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset\n");
        goto error;
    }

    if ((dset_id4 = H5Dcreate2(group_id, DATASET_PROPERTY_LIST_TEST_DSET_NAME4, dset_dtype4,
            space_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset\n");
        goto error;
    }

    if (H5Pclose(dapl_id1) < 0)
        TEST_ERROR

    /* Try to receive copies of the two property lists, one which has the property set and one which does not */
    if ((dapl_id1 = H5Dget_access_plist(dset_id3)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get property list\n");
        goto error;
    }

    if ((dapl_id2 = H5Dget_access_plist(dset_id4)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get property list\n");
        goto error;
    }

    /* Ensure that property list 1 has the property set and property list 2 does not */
    {
        ssize_t buf_size = 0;

        if ((buf_size = H5Pget_efile_prefix(dapl_id1, NULL, 0)) < 0) {
            H5_FAILED();
            HDprintf("    couldn't retrieve size for property value buffer\n");
            goto error;
        }

        if (NULL == (tmp_prefix = (char *) HDcalloc(1, (size_t) buf_size + 1)))
            TEST_ERROR

        if (H5Pget_efile_prefix(dapl_id1, tmp_prefix, (size_t) buf_size + 1) < 0) {
            H5_FAILED();
            HDprintf("    couldn't retrieve property list value\n");
            goto error;
        }

        if (HDstrcmp(tmp_prefix, path_prefix)) {
            H5_FAILED();
            HDprintf("    DAPL values were incorrect!\n");
            goto error;
        }

        HDmemset(tmp_prefix, 0, (size_t) buf_size + 1);

        if (H5Pget_efile_prefix(dapl_id2, tmp_prefix, (size_t) buf_size) < 0) {
            H5_FAILED();
            HDprintf("    couldn't retrieve property list value\n");
            goto error;
        }

        if (!HDstrcmp(tmp_prefix, path_prefix)) {
            H5_FAILED();
            HDprintf("    DAPL property value was set!\n");
            goto error;
        }
    }

    PASSED();

    TESTING_2("H5Dget_create_plist after re-opening a dataset")

    /* Now close the property lists and datasets and see if we can still retrieve copies of
     * the property lists upon opening (instead of creating) a dataset
     */
    if (H5Pclose(dcpl_id1) < 0)
        TEST_ERROR
    if (H5Pclose(dcpl_id2) < 0)
        TEST_ERROR
    if (H5Pclose(dapl_id1) < 0)
        TEST_ERROR
    if (H5Pclose(dapl_id2) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id1) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id2) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id3) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id4) < 0)
        TEST_ERROR

    if ((dset_id1 = H5Dopen2(group_id, DATASET_PROPERTY_LIST_TEST_DSET_NAME1, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open dataset\n");
        goto error;
    }

    if ((dset_id2 = H5Dopen2(group_id, DATASET_PROPERTY_LIST_TEST_DSET_NAME2, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open dataset\n");
        goto error;
    }

    if ((dset_id3 = H5Dopen2(group_id, DATASET_PROPERTY_LIST_TEST_DSET_NAME3, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open dataset\n");
        goto error;
    }

    if ((dset_id4 = H5Dopen2(group_id, DATASET_PROPERTY_LIST_TEST_DSET_NAME4, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open dataset\n");
        goto error;
    }

    if ((dcpl_id1 = H5Dget_create_plist(dset_id1)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get property list\n");
        goto error;
    }

    if ((dcpl_id2 = H5Dget_create_plist(dset_id2)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get property list\n");
        goto error;
    }

    if ((dapl_id1 = H5Dget_access_plist(dset_id3)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get property list\n");
        goto error;
    }

    if ((dapl_id2 = H5Dget_create_plist(dset_id4)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get property list\n");
        goto error;
    }

    /* Ensure that property list 1 has the property set and property list 2 does not */
    {
        hsize_t tmp_chunk_dims[DATASET_PROPERTY_LIST_TEST_SPACE_RANK];

        HDmemset(tmp_chunk_dims, 0, sizeof(tmp_chunk_dims));

        if (H5Pget_chunk(dcpl_id1, DATASET_PROPERTY_LIST_TEST_SPACE_RANK, tmp_chunk_dims) < 0) {
            H5_FAILED();
            HDprintf("    couldn't get DCPL property value\n");
            goto error;
        }

        for (i = 0; i < DATASET_PROPERTY_LIST_TEST_SPACE_RANK; i++)
            if (tmp_chunk_dims[i] != chunk_dims[i]) {
                H5_FAILED();
                HDprintf("    DCPL property values were incorrect\n");
                goto error;
            }

        H5E_BEGIN_TRY {
            err_ret = H5Pget_chunk(dcpl_id2, DATASET_PROPERTY_LIST_TEST_SPACE_RANK, tmp_chunk_dims);
        } H5E_END_TRY;

        if (err_ret >= 0) {
            H5_FAILED();
            HDprintf("    property list 2 shouldn't have had chunk dimensionality set (not a chunked layout)\n");
            goto error;
        }
    }

    if (tmp_prefix) {
        HDfree(tmp_prefix);
        tmp_prefix = NULL;
    }

    if (H5Pclose(dcpl_id1) < 0)
        TEST_ERROR
    if (H5Pclose(dcpl_id2) < 0)
        TEST_ERROR
    if (H5Pclose(dapl_id1) < 0)
        TEST_ERROR
    if (H5Pclose(dapl_id2) < 0)
        TEST_ERROR
    if (H5Sclose(space_id) < 0)
        TEST_ERROR
    if (H5Tclose(dset_dtype1) < 0)
        TEST_ERROR
    if (H5Tclose(dset_dtype2) < 0)
        TEST_ERROR
    if (H5Tclose(dset_dtype3) < 0)
        TEST_ERROR
    if (H5Tclose(dset_dtype4) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id1) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id2) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id3) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id4) < 0)
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
        if (tmp_prefix) HDfree(tmp_prefix);
        H5Pclose(dcpl_id1);
        H5Pclose(dcpl_id2);
        H5Pclose(dapl_id1);
        H5Pclose(dapl_id2);
        H5Sclose(space_id);
        H5Tclose(dset_dtype1);
        H5Tclose(dset_dtype2);
        H5Tclose(dset_dtype3);
        H5Tclose(dset_dtype4);
        H5Dclose(dset_id1);
        H5Dclose(dset_id2);
        H5Dclose(dset_id3);
        H5Dclose(dset_id4);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test for H5Dget_storage_size.
 */
static int
test_get_dataset_storage_size(void)
{
    TESTING("H5Dget_storage_size")

    SKIPPED();

    return 0;
}

/*
 * A test to check that a dataset's storage size can't
 * be retrieved when H5Dget_storage_size is passed
 * invalid parameters.
 */
static int
test_get_dataset_storage_size_invalid_params(void)
{
    TESTING("H5Dget_storage_size with invalid parameters")

    SKIPPED();

    return 0;
}

/*
 * A test for H5Dget_chunk_storage_size.
 */
static int
test_get_dataset_chunk_storage_size(void)
{
    TESTING("H5Dget_chunk_storage_size")

    SKIPPED();

    return 0;
}

/*
 * A test to check that the size of an allocated chunk in
 * a dataset can't be retrieved when H5Dget_chunk_storage_size
 * is passed invalid parameters.
 */
static int
test_get_dataset_chunk_storage_size_invalid_params(void)
{
    TESTING("H5Dget_chunk_storage_size with invalid parameters")

    SKIPPED();

    return 0;
}

/*
 * A test for H5Dget_offset.
 */
static int
test_get_dataset_offset(void)
{
    TESTING("H5Dget_offset")

    SKIPPED();

    return 0;
}

/*
 * A test to check that a dataset's offset can't be
 * retrieved when H5Dget_offset is passed invalid
 * parameters.
 */
static int
test_get_dataset_offset_invalid_params(void)
{
    TESTING("H5Dget_offset with invalid parameters")

    SKIPPED();

    return 0;
}

/*
 * A test to check that a small amount of data can be
 * read back from a dataset using an H5S_ALL selection
 * and then verified.
 *
 * XXX: Add dataset write and data verification.
 */
static int
test_read_dataset_small_all(void)
{
    hsize_t  dims[DATASET_SMALL_READ_TEST_ALL_DSET_SPACE_RANK] = { 10, 5, 3 };
    size_t   i, data_size;
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t    dset_id = H5I_INVALID_HID;
    hid_t    fspace_id = H5I_INVALID_HID;
    void    *read_buf = NULL;

    TESTING("small read from dataset with H5S_ALL")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_SMALL_READ_TEST_ALL_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATASET_SMALL_READ_TEST_ALL_GROUP_NAME);
        goto error;
    }

    if ((fspace_id = H5Screate_simple(DATASET_SMALL_READ_TEST_ALL_DSET_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, DATASET_SMALL_READ_TEST_ALL_DSET_NAME, DATASET_SMALL_READ_TEST_ALL_DSET_DTYPE,
            fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_SMALL_READ_TEST_ALL_DSET_NAME);
        goto error;
    }

    for (i = 0, data_size = 1; i < DATASET_SMALL_READ_TEST_ALL_DSET_SPACE_RANK; i++)
        data_size *= dims[i];
    data_size *= DATASET_SMALL_READ_TEST_ALL_DSET_DTYPESIZE;

    if (NULL == (read_buf = HDmalloc(data_size)))
        TEST_ERROR

    if (H5Dread(dset_id, DATASET_SMALL_READ_TEST_ALL_DSET_DTYPE, H5S_ALL, H5S_ALL, H5P_DEFAULT, read_buf) < 0) {
        H5_FAILED();
        HDprintf("    couldn't read from dataset '%s'\n", DATASET_SMALL_READ_TEST_ALL_DSET_NAME);
        goto error;
    }

    if (read_buf) {
        HDfree(read_buf);
        read_buf = NULL;
    }

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
        if (read_buf) HDfree(read_buf);
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
 * A test to check that a small amount of data can be
 * read back from a dataset using a hyperslab selection
 * and then verified.
 *
 * XXX: Add dataset write and data verification.
 */
static int
test_read_dataset_small_hyperslab(void)
{
    hsize_t  start[DATASET_SMALL_READ_TEST_HYPERSLAB_DSET_SPACE_RANK];
    hsize_t  stride[DATASET_SMALL_READ_TEST_HYPERSLAB_DSET_SPACE_RANK];
    hsize_t  count[DATASET_SMALL_READ_TEST_HYPERSLAB_DSET_SPACE_RANK];
    hsize_t  block[DATASET_SMALL_READ_TEST_HYPERSLAB_DSET_SPACE_RANK];
    hsize_t  dims[DATASET_SMALL_READ_TEST_HYPERSLAB_DSET_SPACE_RANK] = { 10, 5, 3 };
    size_t   i, data_size;
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t    dset_id = H5I_INVALID_HID;
    hid_t    mspace_id = H5I_INVALID_HID, fspace_id = H5I_INVALID_HID;
    void    *read_buf = NULL;

    TESTING("small read from dataset with a hyperslab selection")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_SMALL_READ_TEST_HYPERSLAB_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATASET_SMALL_READ_TEST_HYPERSLAB_GROUP_NAME);
        goto error;
    }

    if ((fspace_id = H5Screate_simple(DATASET_SMALL_READ_TEST_HYPERSLAB_DSET_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR
    if ((mspace_id = H5Screate_simple(DATASET_SMALL_READ_TEST_HYPERSLAB_DSET_SPACE_RANK - 1, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, DATASET_SMALL_READ_TEST_HYPERSLAB_DSET_NAME, DATASET_SMALL_READ_TEST_HYPERSLAB_DSET_DTYPE,
            fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_SMALL_READ_TEST_HYPERSLAB_DSET_NAME);
        goto error;
    }

    for (i = 0; i < DATASET_SMALL_READ_TEST_HYPERSLAB_DSET_SPACE_RANK; i++) {
        start[i] = 0;
        stride[i] = 1;
        count[i] = dims[i];
        block[i] = 1;
    }

    count[2] = 1;

    if (H5Sselect_hyperslab(fspace_id, H5S_SELECT_SET, start, stride, count, block) < 0)
        TEST_ERROR

    for (i = 0, data_size = 1; i < DATASET_SMALL_READ_TEST_HYPERSLAB_DSET_SPACE_RANK - 1; i++)
        data_size *= dims[i];
    data_size *= DATASET_SMALL_READ_TEST_HYPERSLAB_DSET_DTYPESIZE;

    if (NULL == (read_buf = HDmalloc(data_size)))
        TEST_ERROR

    if (H5Dread(dset_id, DATASET_SMALL_READ_TEST_HYPERSLAB_DSET_DTYPE, mspace_id, fspace_id, H5P_DEFAULT, read_buf) < 0) {
        H5_FAILED();
        HDprintf("    couldn't read from dataset '%s'\n", DATASET_SMALL_READ_TEST_HYPERSLAB_DSET_NAME);
        goto error;
    }

    if (read_buf) {
        HDfree(read_buf);
        read_buf = NULL;
    }

    if (H5Sclose(mspace_id) < 0)
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
        if (read_buf) HDfree(read_buf);
        H5Sclose(mspace_id);
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
 * A test to check that a small amount of data can be
 * read back from a dataset using a point selection and
 * then verified.
 *
 * XXX: Add dataset write and data verification.
 */
static int
test_read_dataset_small_point_selection(void)
{
    hsize_t  points[DATASET_SMALL_READ_TEST_POINT_SELECTION_NUM_POINTS * DATASET_SMALL_READ_TEST_POINT_SELECTION_DSET_SPACE_RANK];
    hsize_t  dims[DATASET_SMALL_READ_TEST_POINT_SELECTION_DSET_SPACE_RANK] = { 10, 10, 10 };
    hsize_t  mspace_dims[] = { DATASET_SMALL_READ_TEST_POINT_SELECTION_NUM_POINTS };
    size_t   i, data_size;
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t    dset_id = H5I_INVALID_HID;
    hid_t    fspace_id = H5I_INVALID_HID;
    hid_t    mspace_id = H5I_INVALID_HID;
    void    *data = NULL;

    TESTING("small read from dataset with a point selection")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_SMALL_READ_TEST_POINT_SELECTION_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATASET_SMALL_READ_TEST_POINT_SELECTION_GROUP_NAME);
        goto error;
    }

    if ((fspace_id = H5Screate_simple(DATASET_SMALL_READ_TEST_POINT_SELECTION_DSET_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR
    if ((mspace_id = H5Screate_simple(1, mspace_dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, DATASET_SMALL_READ_TEST_POINT_SELECTION_DSET_NAME, DATASET_SMALL_READ_TEST_POINT_SELECTION_DSET_DTYPE,
            fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_SMALL_READ_TEST_POINT_SELECTION_DSET_NAME);
        goto error;
    }

    data_size = DATASET_SMALL_READ_TEST_POINT_SELECTION_NUM_POINTS * DATASET_SMALL_READ_TEST_POINT_SELECTION_DSET_DTYPESIZE;

    if (NULL == (data = HDmalloc(data_size)))
        TEST_ERROR

    for (i = 0; i < DATASET_SMALL_READ_TEST_POINT_SELECTION_NUM_POINTS; i++) {
        size_t j;

        for (j = 0; j < DATASET_SMALL_READ_TEST_POINT_SELECTION_DSET_SPACE_RANK; j++)
            points[(i * DATASET_SMALL_READ_TEST_POINT_SELECTION_DSET_SPACE_RANK) + j] = i;
    }

    if (H5Sselect_elements(fspace_id, H5S_SELECT_SET, DATASET_SMALL_READ_TEST_POINT_SELECTION_NUM_POINTS, points) < 0) {
        H5_FAILED();
        HDprintf("    couldn't select points\n");
        goto error;
    }

    if (H5Dread(dset_id, DATASET_SMALL_READ_TEST_POINT_SELECTION_DSET_DTYPE, mspace_id, fspace_id, H5P_DEFAULT, data) < 0) {
        H5_FAILED();
        HDprintf("    couldn't read from dataset '%s'\n", DATASET_SMALL_READ_TEST_POINT_SELECTION_DSET_NAME);
        goto error;
    }

    if (data) {
        HDfree(data);
        data = NULL;
    }

    if (H5Sclose(mspace_id) < 0)
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
        if (data) HDfree(data);
        H5Sclose(mspace_id);
        H5Sclose(fspace_id);
        H5Dclose(dset_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

#ifndef NO_LARGE_TESTS
/*
 * A test to check that a large amount of data can be
 * read back from a dataset using an H5S_ALL selection
 * and then verified.
 *
 * XXX: Add dataset write and data verification.
 */
static int
test_read_dataset_large_all(void)
{
    hsize_t  dims[DATASET_LARGE_READ_TEST_ALL_DSET_SPACE_RANK] = { 600, 600, 600 };
    size_t   i, data_size;
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t    dset_id = H5I_INVALID_HID;
    hid_t    fspace_id = H5I_INVALID_HID;
    void    *read_buf = NULL;

    TESTING("large read from dataset with H5S_ALL")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_LARGE_READ_TEST_ALL_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATASET_LARGE_READ_TEST_ALL_GROUP_NAME);
        goto error;
    }

    if ((fspace_id = H5Screate_simple(DATASET_LARGE_READ_TEST_ALL_DSET_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, DATASET_LARGE_READ_TEST_ALL_DSET_NAME, DATASET_LARGE_READ_TEST_ALL_DSET_DTYPE,
            fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_LARGE_READ_TEST_ALL_DSET_NAME);
        goto error;
    }

    for (i = 0, data_size = 1; i < DATASET_LARGE_READ_TEST_ALL_DSET_SPACE_RANK; i++)
        data_size *= dims[i];
    data_size *= DATASET_LARGE_READ_TEST_ALL_DSET_DTYPESIZE;

    if (NULL == (read_buf = HDmalloc(data_size)))
        TEST_ERROR

    if (H5Dread(dset_id, DATASET_LARGE_READ_TEST_ALL_DSET_DTYPE, H5S_ALL, H5S_ALL, H5P_DEFAULT, read_buf) < 0) {
        H5_FAILED();
        HDprintf("    couldn't read from dataset '%s'\n", DATASET_LARGE_READ_TEST_ALL_DSET_NAME);
        goto error;
    }

    if (read_buf) {
        HDfree(read_buf);
        read_buf = NULL;
    }

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
        if (read_buf) HDfree(read_buf);
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
 * A test to check that a large amount of data can be
 * read back from a dataset using a hyperslab selection
 * and then verified.
 *
 * XXX: Add dataset write and data verification.
 */
static int
test_read_dataset_large_hyperslab(void)
{
    hsize_t  start[DATASET_LARGE_READ_TEST_HYPERSLAB_DSET_SPACE_RANK];
    hsize_t  stride[DATASET_LARGE_READ_TEST_HYPERSLAB_DSET_SPACE_RANK];
    hsize_t  count[DATASET_LARGE_READ_TEST_HYPERSLAB_DSET_SPACE_RANK];
    hsize_t  block[DATASET_LARGE_READ_TEST_HYPERSLAB_DSET_SPACE_RANK];
    hsize_t  dims[DATASET_LARGE_READ_TEST_HYPERSLAB_DSET_SPACE_RANK] = { 600, 600, 600 };
    size_t   i, data_size;
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t    dset_id = H5I_INVALID_HID;
    hid_t    mspace_id = H5I_INVALID_HID, fspace_id = H5I_INVALID_HID;
    void    *read_buf = NULL;

    TESTING("large read from dataset with a hyperslab selection")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0){
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_LARGE_READ_TEST_HYPERSLAB_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATASET_LARGE_READ_TEST_HYPERSLAB_GROUP_NAME);
        goto error;
    }

    if ((fspace_id = H5Screate_simple(DATASET_LARGE_READ_TEST_HYPERSLAB_DSET_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR
    if ((mspace_id = H5Screate_simple(DATASET_LARGE_READ_TEST_HYPERSLAB_DSET_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, DATASET_LARGE_READ_TEST_HYPERSLAB_DSET_NAME, DATASET_LARGE_READ_TEST_HYPERSLAB_DSET_DTYPE,
            fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_LARGE_READ_TEST_HYPERSLAB_DSET_NAME);
        goto error;
    }

    for (i = 0; i < DATASET_LARGE_READ_TEST_HYPERSLAB_DSET_SPACE_RANK; i++) {
        start[i] = 0;
        stride[i] = 1;
        count[i] = dims[i];
        block[i] = 1;
    }

    if (H5Sselect_hyperslab(fspace_id, H5S_SELECT_SET, start, stride, count, block) < 0)
        TEST_ERROR

    for (i = 0, data_size = 1; i < DATASET_LARGE_READ_TEST_HYPERSLAB_DSET_SPACE_RANK; i++)
        data_size *= dims[i];
    data_size *= DATASET_LARGE_READ_TEST_HYPERSLAB_DSET_DTYPESIZE;

    if (NULL == (read_buf = HDmalloc(data_size)))
        TEST_ERROR

    if (H5Dread(dset_id, DATASET_LARGE_READ_TEST_HYPERSLAB_DSET_DTYPE, mspace_id, fspace_id, H5P_DEFAULT, read_buf) < 0) {
        H5_FAILED();
        HDprintf("    couldn't read from dataset '%s'\n", DATASET_LARGE_READ_TEST_HYPERSLAB_DSET_NAME);
        goto error;
    }

    if (read_buf) {
        HDfree(read_buf);
        read_buf = NULL;
    }

    if (H5Sclose(mspace_id) < 0)
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
        H5Sclose(mspace_id);
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
 * A test to check that a large amount of data can be
 * read back from a dataset using a large point selection
 * and then verified.
 *
 * XXX: Test takes up significant amounts of memory.
 *
 * XXX: Add dataset write and data verification.
 */
static int
test_read_dataset_large_point_selection(void)
{
    hsize_t *points = NULL;
    hsize_t  dims[DATASET_LARGE_READ_TEST_POINT_SELECTION_DSET_SPACE_RANK] = { 225000000 };
    size_t   i, data_size;
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t    dset_id = H5I_INVALID_HID;
    hid_t    fspace_id = H5I_INVALID_HID;
    void    *data = NULL;

    TESTING("large read from dataset with a point selection")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_LARGE_READ_TEST_POINT_SELECTION_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATASET_LARGE_READ_TEST_POINT_SELECTION_GROUP_NAME);
        goto error;
    }

    if ((fspace_id = H5Screate_simple(DATASET_LARGE_READ_TEST_POINT_SELECTION_DSET_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, DATASET_LARGE_READ_TEST_POINT_SELECTION_DSET_NAME, DATASET_LARGE_READ_TEST_POINT_SELECTION_DSET_DTYPE,
            fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_LARGE_READ_TEST_POINT_SELECTION_DSET_NAME);
        goto error;
    }

    for (i = 0, data_size = 1; i < DATASET_LARGE_READ_TEST_POINT_SELECTION_DSET_SPACE_RANK; i++)
        data_size *= dims[i];
    data_size *= DATASET_LARGE_READ_TEST_POINT_SELECTION_DSET_DTYPESIZE;

    if (NULL == (data = HDmalloc(data_size)))
        TEST_ERROR
    if (NULL == (points = HDmalloc((data_size / DATASET_LARGE_READ_TEST_POINT_SELECTION_DSET_DTYPESIZE)
           * ((DATASET_LARGE_READ_TEST_POINT_SELECTION_DSET_SPACE_RANK) * (sizeof(hsize_t))))))
        TEST_ERROR

    /* Select the entire dataspace */
    for (i = 0; i < data_size / DATASET_LARGE_READ_TEST_POINT_SELECTION_DSET_DTYPESIZE; i++) {
        points[i] = i;
    }

    if (H5Sselect_elements(fspace_id, H5S_SELECT_SET, data_size / DATASET_LARGE_READ_TEST_POINT_SELECTION_DSET_DTYPESIZE, points) < 0) {
        H5_FAILED();
        HDprintf("    couldn't select points\n");
        goto error;
    }

    if (H5Dread(dset_id, DATASET_LARGE_READ_TEST_POINT_SELECTION_DSET_DTYPE, H5S_ALL, fspace_id, H5P_DEFAULT, data) < 0) {
        H5_FAILED();
        HDprintf("    couldn't read from dataset '%s'\n", DATASET_LARGE_READ_TEST_POINT_SELECTION_DSET_NAME);
        goto error;
    }

    if (data) {
        HDfree(data);
        data = NULL;
    }

    if (points) {
        HDfree(points);
        points = NULL;
    }

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
        if (data) HDfree(data);
        if (points) HDfree(points);
        H5Sclose(fspace_id);
        H5Dclose(dset_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}
#endif

/*
 * A test to check that data can't be read from a
 * dataset when H5Dread is passed invalid parameters.
 */
static int
test_read_dataset_invalid_params(void)
{
    hsize_t  dims[DATASET_READ_INVALID_PARAMS_TEST_DSET_SPACE_RANK] = { 10, 5, 3 };
    herr_t   err_ret = -1;
    size_t   i, data_size;
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t    dset_id = H5I_INVALID_HID;
    hid_t    fspace_id = H5I_INVALID_HID;
    void    *read_buf = NULL;

    TESTING("H5Dread with invalid parameters"); HDputs("");

    TESTING_2("H5Dread with an invalid dataset ID")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_READ_INVALID_PARAMS_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATASET_READ_INVALID_PARAMS_TEST_GROUP_NAME);
        goto error;
    }

    if ((fspace_id = H5Screate_simple(DATASET_READ_INVALID_PARAMS_TEST_DSET_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, DATASET_READ_INVALID_PARAMS_TEST_DSET_NAME, DATASET_READ_INVALID_PARAMS_TEST_DSET_DTYPE,
            fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_READ_INVALID_PARAMS_TEST_DSET_NAME);
        goto error;
    }

    for (i = 0, data_size = 1; i < DATASET_READ_INVALID_PARAMS_TEST_DSET_SPACE_RANK; i++)
        data_size *= dims[i];
    data_size *= DATASET_READ_INVALID_PARAMS_TEST_DSET_DTYPESIZE;

    if (NULL == (read_buf = HDmalloc(data_size)))
        TEST_ERROR

    H5E_BEGIN_TRY {
        err_ret = H5Dread(H5I_INVALID_HID, DATASET_READ_INVALID_PARAMS_TEST_DSET_DTYPE, H5S_ALL, H5S_ALL, H5P_DEFAULT, read_buf);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    read from dataset using H5Dread with an invalid dataset ID!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dread with an invalid memory datatype")

    H5E_BEGIN_TRY {
        err_ret = H5Dread(dset_id, H5I_INVALID_HID, H5S_ALL, H5S_ALL, H5P_DEFAULT, read_buf);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    read from dataset using H5Dread with an invalid memory datatype!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dread with an invalid memory dataspace")

    H5E_BEGIN_TRY {
        err_ret = H5Dread(dset_id, DATASET_READ_INVALID_PARAMS_TEST_DSET_DTYPE, H5I_INVALID_HID, H5S_ALL, H5P_DEFAULT, read_buf);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    read from dataset using H5Dread with an invalid memory dataspace!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dread with an invalid file dataspace")

    H5E_BEGIN_TRY {
        err_ret = H5Dread(dset_id, DATASET_READ_INVALID_PARAMS_TEST_DSET_DTYPE, H5S_ALL, H5I_INVALID_HID, H5P_DEFAULT, read_buf);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    read from dataset using H5Dread with an invalid file dataspace!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dread with an invalid DXPL")

    H5E_BEGIN_TRY {
        err_ret = H5Dread(dset_id, DATASET_READ_INVALID_PARAMS_TEST_DSET_DTYPE, H5S_ALL, H5S_ALL, H5I_INVALID_HID, read_buf);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    read from dataset using H5Dread with an invalid DXPL!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dread with an invalid data buffer")

    H5E_BEGIN_TRY {
        err_ret = H5Dread(dset_id, DATASET_READ_INVALID_PARAMS_TEST_DSET_DTYPE, H5S_ALL, H5S_ALL, H5P_DEFAULT, NULL);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    read from dataset using H5Dread with an invalid data buffer!\n");
        goto error;
    }

    if (read_buf) {
        HDfree(read_buf);
        read_buf = NULL;
    }

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
        if (read_buf) HDfree(read_buf);
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
 * A test to check that a small write can be
 * made to a dataset using an H5S_ALL selection.
 */
static int
test_write_dataset_small_all(void)
{
    hssize_t  space_npoints;
    hsize_t   dims[DATASET_SMALL_WRITE_TEST_ALL_DSET_SPACE_RANK] = { 10, 5, 3 };
    size_t    i;
    hid_t     file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t     container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t     dset_id = H5I_INVALID_HID;
    hid_t     fspace_id = H5I_INVALID_HID;
    void     *data = NULL;

    TESTING("small write to dataset with H5S_ALL")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_SMALL_WRITE_TEST_ALL_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATASET_SMALL_WRITE_TEST_ALL_GROUP_NAME);
        goto error;
    }

    if ((fspace_id = H5Screate_simple(DATASET_SMALL_WRITE_TEST_ALL_DSET_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, DATASET_SMALL_WRITE_TEST_ALL_DSET_NAME, DATASET_SMALL_WRITE_TEST_ALL_DSET_DTYPE,
            fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_SMALL_WRITE_TEST_ALL_DSET_NAME);
        goto error;
    }

    /* Close the dataset and dataspace to ensure that writing works correctly in this manner */
    if (H5Dclose(dset_id) < 0)
        TEST_ERROR;
    if (H5Sclose(fspace_id) < 0)
        TEST_ERROR

    if ((dset_id = H5Dopen2(group_id, DATASET_SMALL_WRITE_TEST_ALL_DSET_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open dataset '%s'\n", DATASET_SMALL_WRITE_TEST_ALL_DSET_NAME);
        goto error;
    }

    if ((fspace_id = H5Dget_space(dset_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get dataset dataspace\n");
        goto error;
    }

    if ((space_npoints = H5Sget_simple_extent_npoints(fspace_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get dataspace num points\n");
        goto error;
    }

    if (NULL == (data = HDmalloc((hsize_t) space_npoints * DATASET_SMALL_WRITE_TEST_ALL_DSET_DTYPESIZE)))
        TEST_ERROR

    for (i = 0; i < (hsize_t) space_npoints; i++)
        ((int *) data)[i] = (int) i;

    if (H5Dwrite(dset_id, DATASET_SMALL_WRITE_TEST_ALL_DSET_DTYPE, H5S_ALL, H5S_ALL, H5P_DEFAULT, data) < 0) {
        H5_FAILED();
        HDprintf("    couldn't write to dataset '%s'\n", DATASET_SMALL_WRITE_TEST_ALL_DSET_NAME);
        goto error;
    }

    if (data) {
        HDfree(data);
        data = NULL;
    }

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
        if (data) HDfree(data);
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
 * A test to check that a small write can be made
 * to a dataset using a hyperslab selection.
 */
static int
test_write_dataset_small_hyperslab(void)
{
    hsize_t  start[DATASET_SMALL_WRITE_TEST_HYPERSLAB_DSET_SPACE_RANK];
    hsize_t  stride[DATASET_SMALL_WRITE_TEST_HYPERSLAB_DSET_SPACE_RANK];
    hsize_t  count[DATASET_SMALL_WRITE_TEST_HYPERSLAB_DSET_SPACE_RANK];
    hsize_t  block[DATASET_SMALL_WRITE_TEST_HYPERSLAB_DSET_SPACE_RANK];
    hsize_t  dims[DATASET_SMALL_WRITE_TEST_HYPERSLAB_DSET_SPACE_RANK] = { 10, 5, 3 };
    size_t   i, data_size;
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t    dset_id = H5I_INVALID_HID;
    hid_t    mspace_id = H5I_INVALID_HID, fspace_id = H5I_INVALID_HID;
    void    *data = NULL;

    TESTING("small write to dataset with a hyperslab selection")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_SMALL_WRITE_TEST_HYPERSLAB_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATASET_SMALL_WRITE_TEST_HYPERSLAB_GROUP_NAME);
        goto error;
    }

    if ((fspace_id = H5Screate_simple(DATASET_SMALL_WRITE_TEST_HYPERSLAB_DSET_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR
    if ((mspace_id = H5Screate_simple(DATASET_SMALL_WRITE_TEST_HYPERSLAB_DSET_SPACE_RANK - 1, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, DATASET_SMALL_WRITE_TEST_HYPERSLAB_DSET_NAME, DATASET_SMALL_WRITE_TEST_HYPERSLAB_DSET_DTYPE,
            fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_SMALL_WRITE_TEST_HYPERSLAB_DSET_NAME);
        goto error;
    }

    for (i = 0, data_size = 1; i < DATASET_SMALL_WRITE_TEST_HYPERSLAB_DSET_SPACE_RANK - 1; i++)
        data_size *= dims[i];
    data_size *= DATASET_SMALL_WRITE_TEST_HYPERSLAB_DSET_DTYPESIZE;

    if (NULL == (data = HDmalloc(data_size)))
        TEST_ERROR

    for (i = 0; i < data_size / DATASET_SMALL_WRITE_TEST_HYPERSLAB_DSET_DTYPESIZE; i++)
        ((int *) data)[i] = (int) i;

    for (i = 0; i < DATASET_SMALL_WRITE_TEST_HYPERSLAB_DSET_SPACE_RANK; i++) {
        start[i] = 0;
        stride[i] = 1;
        count[i] = dims[i];
        block[i] = 1;
    }

    count[2] = 1;

    if (H5Sselect_hyperslab(fspace_id, H5S_SELECT_SET, start, stride, count, block) < 0)
        TEST_ERROR

    if (H5Dwrite(dset_id, DATASET_SMALL_WRITE_TEST_HYPERSLAB_DSET_DTYPE, mspace_id, fspace_id, H5P_DEFAULT, data) < 0) {
        H5_FAILED();
        HDprintf("    couldn't write to dataset '%s'\n", DATASET_SMALL_WRITE_TEST_HYPERSLAB_DSET_NAME);
        goto error;
    }

    if (data) {
        HDfree(data);
        data = NULL;
    }

    if (H5Sclose(mspace_id) < 0)
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
        if (data) HDfree(data);
        H5Sclose(mspace_id);
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
 * A test to check that a small write can be made
 * to a dataset using a point selection.
 */
static int
test_write_dataset_small_point_selection(void)
{
    hsize_t  points[DATASET_SMALL_WRITE_TEST_POINT_SELECTION_NUM_POINTS * DATASET_SMALL_WRITE_TEST_POINT_SELECTION_DSET_SPACE_RANK];
    hsize_t  dims[DATASET_SMALL_WRITE_TEST_POINT_SELECTION_DSET_SPACE_RANK] = { 10, 10, 10 };
    hsize_t  mdims[] = { DATASET_SMALL_WRITE_TEST_POINT_SELECTION_NUM_POINTS };
    size_t   i, data_size;
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t    dset_id = H5I_INVALID_HID;
    hid_t    fspace_id = H5I_INVALID_HID;
    hid_t    mspace_id = H5I_INVALID_HID;
    void    *data = NULL;

    TESTING("small write to dataset with a point selection")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_SMALL_WRITE_TEST_POINT_SELECTION_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATASET_SMALL_WRITE_TEST_POINT_SELECTION_GROUP_NAME);
        goto error;
    }

    if ((fspace_id = H5Screate_simple(DATASET_SMALL_WRITE_TEST_POINT_SELECTION_DSET_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR
    if ((mspace_id = H5Screate_simple(1, mdims, NULL)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, DATASET_SMALL_WRITE_TEST_POINT_SELECTION_DSET_NAME, DATASET_SMALL_WRITE_TEST_POINT_SELECTION_DSET_DTYPE,
            fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_SMALL_WRITE_TEST_POINT_SELECTION_DSET_NAME);
        goto error;
    }

    data_size = DATASET_SMALL_WRITE_TEST_POINT_SELECTION_NUM_POINTS * DATASET_SMALL_WRITE_TEST_POINT_SELECTION_DSET_DTYPESIZE;

    if (NULL == (data = HDmalloc(data_size)))
        TEST_ERROR

    for (i = 0; i < data_size / DATASET_SMALL_WRITE_TEST_POINT_SELECTION_DSET_DTYPESIZE; i++)
        ((int *) data)[i] = (int) i;

    for (i = 0; i < DATASET_SMALL_WRITE_TEST_POINT_SELECTION_NUM_POINTS; i++) {
        size_t j;

        for (j = 0; j < DATASET_SMALL_WRITE_TEST_POINT_SELECTION_DSET_SPACE_RANK; j++)
            points[(i * DATASET_SMALL_WRITE_TEST_POINT_SELECTION_DSET_SPACE_RANK) + j] = i;
    }

    if (H5Sselect_elements(fspace_id, H5S_SELECT_SET, DATASET_SMALL_WRITE_TEST_POINT_SELECTION_NUM_POINTS, points) < 0) {
        H5_FAILED();
        HDprintf("    couldn't select points\n");
        goto error;
    }

    if (H5Dwrite(dset_id, DATASET_SMALL_WRITE_TEST_POINT_SELECTION_DSET_DTYPE, mspace_id, fspace_id, H5P_DEFAULT, data) < 0) {
        H5_FAILED();
        HDprintf("    couldn't write to dataset '%s'\n", DATASET_SMALL_WRITE_TEST_POINT_SELECTION_DSET_NAME);
        goto error;
    }

    if (data) {
        HDfree(data);
        data = NULL;
    }

    if (H5Sclose(mspace_id) < 0)
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
        if (data) HDfree(data);
        H5Sclose(mspace_id);
        H5Sclose(fspace_id);
        H5Dclose(dset_id);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

#ifndef NO_LARGE_TESTS
/*
 * A test to check that a large write can be made
 * to a dataset using an H5S_ALL selection.
 */
static int
test_write_dataset_large_all(void)
{
    hssize_t  space_npoints;
    hsize_t   dims[DATASET_LARGE_WRITE_TEST_ALL_DSET_SPACE_RANK] = { 600, 600, 600 };
    size_t    i, data_size;
    hid_t     file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t     container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t     dset_id = H5I_INVALID_HID;
    hid_t     fspace_id = H5I_INVALID_HID;
    void     *data = NULL;

    TESTING("large write to dataset with H5S_ALL")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_LARGE_WRITE_TEST_ALL_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATASET_LARGE_WRITE_TEST_ALL_GROUP_NAME);
        goto error;
    }

    if ((fspace_id = H5Screate_simple(DATASET_LARGE_WRITE_TEST_ALL_DSET_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, DATASET_LARGE_WRITE_TEST_ALL_DSET_NAME, DATASET_LARGE_WRITE_TEST_ALL_DSET_DTYPE,
            fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_LARGE_WRITE_TEST_ALL_DSET_NAME);
        goto error;
    }

    /* Close the dataset and dataspace to ensure that retrieval of file space ID is working */
    if (H5Dclose(dset_id) < 0)
        TEST_ERROR
    if (H5Sclose(fspace_id) < 0)
        TEST_ERROR

    if ((dset_id = H5Dopen2(group_id, DATASET_LARGE_WRITE_TEST_ALL_DSET_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open dataset '%s'\n", DATASET_LARGE_WRITE_TEST_ALL_DSET_NAME);
        goto error;
    }

    if ((fspace_id = H5Dget_space(dset_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get dataset dataspace\n");
        goto error;
    }

    if ((space_npoints = H5Sget_simple_extent_npoints(fspace_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get dataspace num points\n");
        goto error;
    }

    if (NULL == (data = HDmalloc((hsize_t) space_npoints * DATASET_LARGE_WRITE_TEST_ALL_DSET_DTYPESIZE)))
        TEST_ERROR

    for (i = 0; i < (hsize_t) space_npoints; i++)
        ((int *) data)[i] = (int) i;

    if (H5Dwrite(dset_id, DATASET_LARGE_WRITE_TEST_ALL_DSET_DTYPE, H5S_ALL, H5S_ALL, H5P_DEFAULT, data) < 0) {
        H5_FAILED();
        HDprintf("    couldn't write to dataset '%s'\n", DATASET_LARGE_WRITE_TEST_ALL_DSET_NAME);
        goto error;
    }

    if (data) {
        HDfree(data);
        data = NULL;
    }

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
 * A test to check that a large write can be made
 * to a dataset using a hyperslab selection.
 */
static int
test_write_dataset_large_hyperslab(void)
{
    hsize_t  start[DATASET_LARGE_WRITE_TEST_HYPERSLAB_DSET_SPACE_RANK];
    hsize_t  stride[DATASET_LARGE_WRITE_TEST_HYPERSLAB_DSET_SPACE_RANK];
    hsize_t  count[DATASET_LARGE_WRITE_TEST_HYPERSLAB_DSET_SPACE_RANK];
    hsize_t  block[DATASET_LARGE_WRITE_TEST_HYPERSLAB_DSET_SPACE_RANK];
    hsize_t  dims[DATASET_LARGE_WRITE_TEST_HYPERSLAB_DSET_SPACE_RANK] = { 600, 600, 600 };
    size_t   i, data_size;
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t    dset_id = H5I_INVALID_HID;
    hid_t    mspace_id = H5I_INVALID_HID, fspace_id = H5I_INVALID_HID;
    void    *data = NULL;

    TESTING("large write to dataset with a hyperslab selection")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_LARGE_WRITE_TEST_HYPERSLAB_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATASET_LARGE_WRITE_TEST_HYPERSLAB_GROUP_NAME);
        goto error;
    }

    if ((fspace_id = H5Screate_simple(DATASET_LARGE_WRITE_TEST_HYPERSLAB_DSET_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR
    if ((mspace_id = H5Screate_simple(DATASET_LARGE_WRITE_TEST_HYPERSLAB_DSET_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, DATASET_LARGE_WRITE_TEST_HYPERSLAB_DSET_NAME, DATASET_LARGE_WRITE_TEST_HYPERSLAB_DSET_DTYPE,
            fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_LARGE_WRITE_TEST_HYPERSLAB_DSET_NAME);
        goto error;
    }

    for (i = 0, data_size = 1; i < DATASET_LARGE_WRITE_TEST_HYPERSLAB_DSET_SPACE_RANK; i++)
        data_size *= dims[i];
    data_size *= DATASET_LARGE_WRITE_TEST_HYPERSLAB_DSET_DTYPESIZE;

    if (NULL == (data = HDmalloc(data_size)))
        TEST_ERROR

    for (i = 0; i < data_size / DATASET_LARGE_WRITE_TEST_HYPERSLAB_DSET_DTYPESIZE; i++)
        ((int *) data)[i] = (int) i;

    for (i = 0; i < DATASET_LARGE_WRITE_TEST_HYPERSLAB_DSET_SPACE_RANK; i++) {
        start[i] = 0;
        stride[i] = 1;
        count[i] = dims[i];
        block[i] = 1;
    }

    if (H5Sselect_hyperslab(fspace_id, H5S_SELECT_SET, start, stride, count, block) < 0)
        TEST_ERROR

    if (H5Dwrite(dset_id, DATASET_LARGE_WRITE_TEST_HYPERSLAB_DSET_DTYPE, mspace_id, fspace_id, H5P_DEFAULT, data) < 0) {
        H5_FAILED();
        HDprintf("    couldn't write to dataset '%s'\n", DATASET_LARGE_WRITE_TEST_HYPERSLAB_DSET_NAME);
        goto error;
    }

    if (data) {
        HDfree(data);
        data = NULL;
    }

    if (H5Sclose(mspace_id) < 0)
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
        if (data) HDfree(data);
        H5Sclose(mspace_id);
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
 * A test to check that a large write can be made
 * to a dataset using a point selection.
 */
static int
test_write_dataset_large_point_selection(void)
{
    TESTING("large write to dataset with a point selection")

    SKIPPED();

    return 0;

error:
    return 1;
}
#endif

/*
 * A test to ensure that data is read back correctly from
 * a dataset after it has been written.
 */
static int
test_write_dataset_data_verification(void)
{
    hssize_t space_npoints;
    hsize_t  dims[DATASET_DATA_VERIFY_WRITE_TEST_DSET_SPACE_RANK] = { 10, 10, 10 };
    hsize_t  start[DATASET_DATA_VERIFY_WRITE_TEST_DSET_SPACE_RANK];
    hsize_t  stride[DATASET_DATA_VERIFY_WRITE_TEST_DSET_SPACE_RANK];
    hsize_t  count[DATASET_DATA_VERIFY_WRITE_TEST_DSET_SPACE_RANK];
    hsize_t  block[DATASET_DATA_VERIFY_WRITE_TEST_DSET_SPACE_RANK];
    hsize_t  points[DATASET_DATA_VERIFY_WRITE_TEST_NUM_POINTS * DATASET_DATA_VERIFY_WRITE_TEST_DSET_SPACE_RANK];
    size_t   i, data_size;
    hid_t    file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t    container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t    dset_id = H5I_INVALID_HID;
    hid_t    fspace_id = H5I_INVALID_HID;
    hid_t    mspace_id = H5I_INVALID_HID;
    void    *data = NULL;
    void    *write_buf = NULL;
    void    *read_buf = NULL;

    TESTING("verification of dataset data using H5Dwrite then H5Dread"); HDputs("");

    TESTING_2("H5Dwrite using H5S_ALL then H5Dread")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_DATA_VERIFY_WRITE_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATASET_DATA_VERIFY_WRITE_TEST_GROUP_NAME);
        goto error;
    }

    if ((fspace_id = H5Screate_simple(DATASET_DATA_VERIFY_WRITE_TEST_DSET_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, DATASET_DATA_VERIFY_WRITE_TEST_DSET_NAME, DATASET_DATA_VERIFY_WRITE_TEST_DSET_DTYPE,
            fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_DATA_VERIFY_WRITE_TEST_DSET_NAME);
        goto error;
    }

    for (i = 0, data_size = 1; i < DATASET_DATA_VERIFY_WRITE_TEST_DSET_SPACE_RANK; i++)
        data_size *= dims[i];
    data_size *= DATASET_DATA_VERIFY_WRITE_TEST_DSET_DTYPESIZE;

    if (NULL == (data = HDmalloc(data_size)))
        TEST_ERROR

    for (i = 0; i < data_size / DATASET_DATA_VERIFY_WRITE_TEST_DSET_DTYPESIZE; i++)
        ((int *) data)[i] = (int) i;

    if (H5Dwrite(dset_id, DATASET_DATA_VERIFY_WRITE_TEST_DSET_DTYPE, H5S_ALL, H5S_ALL, H5P_DEFAULT, data) < 0) {
        H5_FAILED();
        HDprintf("    couldn't write to dataset '%s'\n", DATASET_DATA_VERIFY_WRITE_TEST_DSET_NAME);
        goto error;
    }

    if (data) {
        HDfree(data);
        data = NULL;
    }

    if (H5Sclose(fspace_id) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id) < 0)
        TEST_ERROR

    if ((dset_id = H5Dopen2(group_id, DATASET_DATA_VERIFY_WRITE_TEST_DSET_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open dataset '%s'\n", DATASET_DATA_VERIFY_WRITE_TEST_DSET_NAME);
        goto error;
    }

    if ((fspace_id = H5Dget_space(dset_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get dataset dataspace\n");
        goto error;
    }

    if ((space_npoints = H5Sget_simple_extent_npoints(fspace_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get dataspace num points\n");
        goto error;
    }

    if (NULL == (data = HDmalloc((hsize_t) space_npoints * DATASET_DATA_VERIFY_WRITE_TEST_DSET_DTYPESIZE)))
        TEST_ERROR

    if (H5Dread(dset_id, DATASET_DATA_VERIFY_WRITE_TEST_DSET_DTYPE, H5S_ALL, H5S_ALL, H5P_DEFAULT, data) < 0) {
        H5_FAILED();
        HDprintf("    couldn't read from dataset '%s'\n", DATASET_DATA_VERIFY_WRITE_TEST_DSET_NAME);
        goto error;
    }

    for (i = 0; i < (hsize_t) space_npoints; i++)
        if (((int *) data)[i] != (int) i) {
            H5_FAILED();
            HDprintf("    H5S_ALL selection data verification failed\n");
            goto error;
        }

    if (data) {
        HDfree(data);
        data = NULL;
    }

    PASSED();

    TESTING_2("H5Dwrite using hyperslab selection then H5Dread")

    data_size = dims[1] * 2 * DATASET_DATA_VERIFY_WRITE_TEST_DSET_DTYPESIZE;

    if (NULL == (write_buf = HDmalloc(data_size)))
        TEST_ERROR

    for (i = 0; i < data_size / DATASET_DATA_VERIFY_WRITE_TEST_DSET_DTYPESIZE; i++)
        ((int *) write_buf)[i] = 56;

    for (i = 0, data_size = 1; i < DATASET_DATA_VERIFY_WRITE_TEST_DSET_SPACE_RANK; i++)
        data_size *= dims[i];
    data_size *= DATASET_DATA_VERIFY_WRITE_TEST_DSET_DTYPESIZE;

    if (NULL == (data = HDmalloc(data_size)))
        TEST_ERROR

    if (H5Dread(dset_id, DATASET_DATA_VERIFY_WRITE_TEST_DSET_DTYPE, H5S_ALL, H5S_ALL, H5P_DEFAULT, data) < 0) {
        H5_FAILED();
        HDprintf("    couldn't read from dataset '%s'\n", DATASET_DATA_VERIFY_WRITE_TEST_DSET_NAME);
        goto error;
    }

    for (i = 0; i < 2; i++) {
        size_t j;

        for (j = 0; j < dims[1]; j++)
            ((int *) data)[(i * dims[1] * dims[2]) + (j * dims[2])] = 56;
    }

    /* Write to first two rows of dataset */
    start[0] = start[1] = start[2] = 0;
    stride[0] = stride[1] = stride[2] = 1;
    count[0] = 2; count[1] = dims[1]; count[2] = 1;
    block[0] = block[1] = block[2] = 1;

    if (H5Sselect_hyperslab(fspace_id, H5S_SELECT_SET, start, stride, count, block) < 0)
        TEST_ERROR

    {
        hsize_t mdims[] = { (hsize_t) 2 * dims[1] };

        if ((mspace_id = H5Screate_simple(1, mdims, NULL)) < 0)
            TEST_ERROR
    }

    if (H5Dwrite(dset_id, DATASET_DATA_VERIFY_WRITE_TEST_DSET_DTYPE, mspace_id, fspace_id, H5P_DEFAULT, write_buf) < 0) {
        H5_FAILED();
        HDprintf("    couldn't write to dataset '%s'\n", DATASET_DATA_VERIFY_WRITE_TEST_DSET_NAME);
        goto error;
    }

    if (H5Sclose(mspace_id) < 0)
        TEST_ERROR
    if (H5Sclose(fspace_id) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id) < 0)
        TEST_ERROR

    if ((dset_id = H5Dopen2(group_id, DATASET_DATA_VERIFY_WRITE_TEST_DSET_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open dataset '%s'\n", DATASET_DATA_VERIFY_WRITE_TEST_DSET_NAME);
        goto error;
    }

    if ((fspace_id = H5Dget_space(dset_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get dataset dataspace\n");
        goto error;
    }

    if ((space_npoints = H5Sget_simple_extent_npoints(fspace_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get dataspace num points\n");
        goto error;
    }

    if (NULL == (read_buf = HDmalloc((hsize_t) space_npoints * DATASET_DATA_VERIFY_WRITE_TEST_DSET_DTYPESIZE)))
        TEST_ERROR

    if (H5Dread(dset_id, DATASET_DATA_VERIFY_WRITE_TEST_DSET_DTYPE, H5S_ALL, H5S_ALL, H5P_DEFAULT, read_buf) < 0) {
        H5_FAILED();
        HDprintf("    couldn't read from dataset '%s'\n", DATASET_DATA_VERIFY_WRITE_TEST_DSET_NAME);
        goto error;
    }

    if (memcmp(data, read_buf, data_size)) {
        H5_FAILED();
        HDprintf("    hyperslab selection data verification failed\n");
        goto error;
    }

    if (data) {
        HDfree(data);
        data = NULL;
    }

    if (write_buf) {
        HDfree(write_buf);
        write_buf = NULL;
    }

    if (read_buf) {
        HDfree(read_buf);
        read_buf = NULL;
    }

    PASSED();

    TESTING_2("H5Dwrite using point selection then H5Dread")

    data_size = DATASET_DATA_VERIFY_WRITE_TEST_NUM_POINTS * DATASET_DATA_VERIFY_WRITE_TEST_DSET_DTYPESIZE;

    if (NULL == (write_buf = HDmalloc(data_size)))
        TEST_ERROR

    for (i = 0; i < data_size / DATASET_DATA_VERIFY_WRITE_TEST_DSET_DTYPESIZE; i++)
        ((int *) write_buf)[i] = 13;

    for (i = 0, data_size = 1; i < DATASET_DATA_VERIFY_WRITE_TEST_DSET_SPACE_RANK; i++)
        data_size *= dims[i];
    data_size *= DATASET_DATA_VERIFY_WRITE_TEST_DSET_DTYPESIZE;

    if (NULL == (data = HDmalloc(data_size)))
        TEST_ERROR

    if (H5Dread(dset_id, DATASET_DATA_VERIFY_WRITE_TEST_DSET_DTYPE, H5S_ALL, H5S_ALL, H5P_DEFAULT, data) < 0) {
        H5_FAILED();
        HDprintf("    couldn't read from dataset '%s'\n", DATASET_DATA_VERIFY_WRITE_TEST_DSET_NAME);
        goto error;
    }

    for (i = 0; i < dims[0]; i++) {
        size_t j;

        for (j = 0; j < dims[1]; j++) {
            size_t k;

            for (k = 0; k < dims[2]; k++) {
                if (i == j && j == k)
                    ((int *) data)[(i * dims[1] * dims[2]) + (j * dims[2]) + k] = 13;
            }
        }
    }


    /* Select a series of 10 points in the dataset */
    for (i = 0; i < DATASET_DATA_VERIFY_WRITE_TEST_NUM_POINTS; i++) {
        size_t j;

        for (j = 0; j < DATASET_DATA_VERIFY_WRITE_TEST_DSET_SPACE_RANK; j++)
            points[(i * DATASET_DATA_VERIFY_WRITE_TEST_DSET_SPACE_RANK) + j] = i;
    }

    if (H5Sselect_elements(fspace_id, H5S_SELECT_SET, DATASET_DATA_VERIFY_WRITE_TEST_NUM_POINTS, points) < 0)
        TEST_ERROR

    {
        hsize_t mdims[] = { (hsize_t) DATASET_DATA_VERIFY_WRITE_TEST_NUM_POINTS };

        if ((mspace_id = H5Screate_simple(1, mdims, NULL)) < 0)
            TEST_ERROR
    }

    if (H5Dwrite(dset_id, DATASET_DATA_VERIFY_WRITE_TEST_DSET_DTYPE, mspace_id, fspace_id, H5P_DEFAULT, write_buf) < 0) {
        H5_FAILED();
        HDprintf("    couldn't write to dataset '%s'\n", DATASET_DATA_VERIFY_WRITE_TEST_DSET_NAME);
        goto error;
    }

    if (H5Sclose(mspace_id) < 0)
        TEST_ERROR
    if (H5Sclose(fspace_id) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id) < 0)
        TEST_ERROR

    if ((dset_id = H5Dopen2(group_id, DATASET_DATA_VERIFY_WRITE_TEST_DSET_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open dataset '%s'\n", DATASET_DATA_VERIFY_WRITE_TEST_DSET_NAME);
        goto error;
    }

    if ((fspace_id = H5Dget_space(dset_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get dataset dataspace\n");
        goto error;
    }

    if ((space_npoints = H5Sget_simple_extent_npoints(fspace_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get dataspace num points\n");
        goto error;
    }

    if (NULL == (read_buf = HDmalloc((hsize_t) space_npoints * DATASET_DATA_VERIFY_WRITE_TEST_DSET_DTYPESIZE)))
        TEST_ERROR

    if (H5Dread(dset_id, DATASET_DATA_VERIFY_WRITE_TEST_DSET_DTYPE, H5S_ALL, H5S_ALL, H5P_DEFAULT, read_buf) < 0) {
        H5_FAILED();
        HDprintf("    couldn't read from dataset '%s'\n", DATASET_DATA_VERIFY_WRITE_TEST_DSET_NAME);
        goto error;
    }

    if (memcmp(data, read_buf, data_size)) {
        H5_FAILED();
        HDprintf("    point selection data verification failed\n");
        goto error;
    }

    if (data) {
        HDfree(data);
        data = NULL;
    }

    if (write_buf) {
        HDfree(write_buf);
        write_buf = NULL;
    }

    if (read_buf) {
        HDfree(read_buf);
        read_buf = NULL;
    }

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
        if (data) HDfree(data);
        if (write_buf) HDfree(write_buf);
        if (read_buf) HDfree(read_buf);
        H5Sclose(mspace_id);
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
 * A test to check that a dataset can't be written to
 * when H5Dwrite is passed invalid parameters.
 */
static int
test_write_dataset_invalid_params(void)
{
    hssize_t  space_npoints;
    hsize_t   dims[DATASET_WRITE_INVALID_PARAMS_TEST_DSET_SPACE_RANK] = { 10, 5, 3 };
    herr_t    err_ret = -1;
    size_t    i;
    hid_t     file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t     container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t     dset_id = H5I_INVALID_HID;
    hid_t     fspace_id = H5I_INVALID_HID;
    void     *data = NULL;

    TESTING("H5Dwrite with invalid parameters"); HDputs("");

    TESTING_2("H5Dwrite with an invalid dataset ID")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_WRITE_INVALID_PARAMS_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATASET_WRITE_INVALID_PARAMS_TEST_GROUP_NAME);
        goto error;
    }

    if ((fspace_id = H5Screate_simple(DATASET_WRITE_INVALID_PARAMS_TEST_DSET_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_id = H5Dcreate2(group_id, DATASET_WRITE_INVALID_PARAMS_TEST_DSET_NAME, DATASET_SMALL_WRITE_TEST_ALL_DSET_DTYPE,
            fspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_WRITE_INVALID_PARAMS_TEST_DSET_NAME);
        goto error;
    }

    if ((space_npoints = H5Sget_simple_extent_npoints(fspace_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't get dataspace num points\n");
        goto error;
    }

    if (NULL == (data = HDmalloc((hsize_t) space_npoints * DATASET_WRITE_INVALID_PARAMS_TEST_DSET_DTYPESIZE)))
        TEST_ERROR

    for (i = 0; i < (hsize_t) space_npoints; i++)
        ((int *) data)[i] = (int) i;

    H5E_BEGIN_TRY {
        err_ret = H5Dwrite(H5I_INVALID_HID, DATASET_WRITE_INVALID_PARAMS_TEST_DSET_DTYPE, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    wrote to dataset using H5Dwrite with an invalid dataset ID!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dwrite with an invalid memory datatype")

    H5E_BEGIN_TRY {
        err_ret = H5Dwrite(dset_id, H5I_INVALID_HID, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    wrote to dataset using H5Dwrite with an invalid memory datatype!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dwrite with an invalid memory dataspace")

    H5E_BEGIN_TRY {
        err_ret = H5Dwrite(dset_id, DATASET_WRITE_INVALID_PARAMS_TEST_DSET_DTYPE, H5I_INVALID_HID, H5S_ALL, H5P_DEFAULT, data);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    wrote to dataset using H5Dwrite with an invalid memory dataspace!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dwrite with an invalid file dataspace")

    H5E_BEGIN_TRY {
        err_ret = H5Dwrite(dset_id, DATASET_WRITE_INVALID_PARAMS_TEST_DSET_DTYPE, H5S_ALL, H5I_INVALID_HID, H5P_DEFAULT, data);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    wrote to dataset using H5Dwrite with an invalid file dataspace!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dwrite with an invalid DXPL")

    H5E_BEGIN_TRY {
        err_ret = H5Dwrite(dset_id, DATASET_WRITE_INVALID_PARAMS_TEST_DSET_DTYPE, H5S_ALL, H5S_ALL, H5I_INVALID_HID, data);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    wrote to dataset using H5Dwrite with an invalid DXPL!\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dwrite with an invalid data buffer")

    H5E_BEGIN_TRY {
        err_ret = H5Dwrite(dset_id, DATASET_WRITE_INVALID_PARAMS_TEST_DSET_DTYPE, H5S_ALL, H5S_ALL, H5P_DEFAULT, NULL);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    wrote to dataset using H5Dwrite with an invalid data buffer!\n");
        goto error;
    }

    if (data) {
        HDfree(data);
        data = NULL;
    }

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
        if (data) HDfree(data);
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
 * A test for H5Diterate.
 */
static int
test_dataset_iterate(void)
{
    TESTING("H5Diterate")

    SKIPPED();

    return 0;
}

/*
 * A test to check that H5Diterate doesn't fail when the
 * selection in the dataset's dataspace is empty.
 */
static int
test_dataset_iterate_empty_selection(void)
{
    TESTING("H5Diterate with an empty selection")

    SKIPPED();

    return 0;
}

/*
 * A test to check that a dataset's dataspace can't be
 * iterated over when H5Diterate is passed invalid parameters.
 */
static int
test_dataset_iterate_invalid_params(void)
{
    TESTING("H5Diterate with invalid parameters")

    SKIPPED();

    return 0;
}

/*
 * A test to check that a chunked dataset's extent can be
 * changed by using H5Dset_extent. This test uses unlimited
 * dimensions for the dataset, so the dimensionality of the
 * dataset may both shrink and grow.
 */
static int
test_dataset_set_extent_chunked_unlimited(void)
{
    hsize_t dims[DATASET_SET_EXTENT_CHUNKED_UNLIMITED_TEST_SPACE_RANK];
    hsize_t max_dims[DATASET_SET_EXTENT_CHUNKED_UNLIMITED_TEST_SPACE_RANK];
    hsize_t chunk_dims[DATASET_SET_EXTENT_CHUNKED_UNLIMITED_TEST_SPACE_RANK];
    hsize_t new_dims[DATASET_SET_EXTENT_CHUNKED_UNLIMITED_TEST_SPACE_RANK];
    size_t  i;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t   dset_id = H5I_INVALID_HID;
    hid_t   dset_dtype = H5I_INVALID_HID;
    hid_t   dcpl_id = H5I_INVALID_HID;
    hid_t   fspace_id = H5I_INVALID_HID;

    TESTING("H5Dset_extent on chunked dataset with unlimited dimensions")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_SET_EXTENT_CHUNKED_UNLIMITED_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATASET_SET_EXTENT_CHUNKED_UNLIMITED_TEST_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < DATASET_SET_EXTENT_CHUNKED_UNLIMITED_TEST_SPACE_RANK; i++) {
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);
        max_dims[i] = H5S_UNLIMITED;
        chunk_dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);
    }

    if ((fspace_id = H5Screate_simple(DATASET_SET_EXTENT_CHUNKED_UNLIMITED_TEST_SPACE_RANK, dims, max_dims)) < 0)
        TEST_ERROR

    if ((dset_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((dcpl_id = H5Pcreate(H5P_DATASET_CREATE)) < 0)
        TEST_ERROR

    if (H5Pset_chunk(dcpl_id, DATASET_SET_EXTENT_CHUNKED_UNLIMITED_TEST_SPACE_RANK, chunk_dims) < 0) {
        H5_FAILED();
        HDprintf("    unable to set dataset chunk dimensionality\n");
        goto error;
    }

    if ((dset_id = H5Dcreate2(group_id, DATASET_SET_EXTENT_CHUNKED_UNLIMITED_TEST_DSET_NAME, dset_dtype,
            fspace_id, H5P_DEFAULT, dcpl_id, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_SET_EXTENT_CHUNKED_UNLIMITED_TEST_DSET_NAME);
        goto error;
    }

    for (i = 0; i < DATASET_SET_EXTENT_CHUNKED_UNLIMITED_TEST_NUM_PASSES; i++) {
        size_t j;

        for (j = 0; j < DATASET_SET_EXTENT_CHUNKED_UNLIMITED_TEST_SPACE_RANK; j++) {
            /* Ensure that the new dimensionality doesn't match the old dimensionality. */
            do {
                new_dims[j] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);
            } while (new_dims[j] == dims[j]);
        }

        if (H5Dset_extent(dset_id, new_dims) < 0) {
            H5_FAILED();
            HDprintf("    failed to set dataset extent\n");
            goto error;
        }

        /* Retrieve the new dimensions of the dataset and ensure they
         * are different from the original.
         */
        if (H5Sclose(fspace_id) < 0)
            TEST_ERROR

        if ((fspace_id = H5Dget_space(dset_id)) < 0) {
            H5_FAILED();
            HDprintf("    failed to retrieve dataset's dataspace\n");
            goto error;
        }

        if (H5Sget_simple_extent_dims(fspace_id, new_dims, NULL) < 0) {
            H5_FAILED();
            HDprintf("    failed to retreive dataset dimensionality\n");
            goto error;
        }

        /*
         * Make sure the dimensions have been changed.
         */
        for (j = 0; j < DATASET_SET_EXTENT_CHUNKED_UNLIMITED_TEST_SPACE_RANK; j++) {
            if (dims[j] == new_dims[j]) {
                H5_FAILED();
                HDprintf("    dataset dimension %llu wasn't changed!\n", (unsigned long long) j);
                goto error;
            }
        }

        /*
         * Remember the current dimensionality of the dataset before
         * changing them again.
         */
        HDmemcpy(dims, new_dims, sizeof(new_dims));
    }

    /*
     * Now close and re-open the dataset each pass to check the persistence
     * of the changes to the dataset's dimensionality.
     */
    for (i = 0; i < DATASET_SET_EXTENT_CHUNKED_UNLIMITED_TEST_NUM_PASSES; i++) {
        size_t j;

        for (j = 0; j < DATASET_SET_EXTENT_CHUNKED_UNLIMITED_TEST_SPACE_RANK; j++) {
            /* Ensure that the new dimensionality doesn't match the old dimensionality. */
            do {
                new_dims[j] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);
            } while (new_dims[j] == dims[j]);
        }

        if (H5Dset_extent(dset_id, new_dims) < 0) {
            H5_FAILED();
            HDprintf("    failed to set dataset extent\n");
            goto error;
        }

        /* Retrieve the new dimensions of the dataset and ensure they
         * are different from the original.
         */
        if (H5Sclose(fspace_id) < 0)
            TEST_ERROR
        if (H5Dclose(dset_id) < 0)
            TEST_ERROR

        if ((dset_id = H5Dopen2(group_id, DATASET_SET_EXTENT_CHUNKED_UNLIMITED_TEST_DSET_NAME, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDprintf("    failed to open dataset '%s'\n", DATASET_SET_EXTENT_CHUNKED_UNLIMITED_TEST_DSET_NAME);
            goto error;
        }

        if ((fspace_id = H5Dget_space(dset_id)) < 0) {
            H5_FAILED();
            HDprintf("    failed to retrieve dataset's dataspace\n");
            goto error;
        }

        if (H5Sget_simple_extent_dims(fspace_id, new_dims, NULL) < 0) {
            H5_FAILED();
            HDprintf("    failed to retreive dataset dimensionality\n");
            goto error;
        }

        /*
         * Make sure the dimensions have been changed.
         */
        for (j = 0; j < DATASET_SET_EXTENT_CHUNKED_UNLIMITED_TEST_SPACE_RANK; j++) {
            if (dims[j] == new_dims[j]) {
                H5_FAILED();
                HDprintf("    dataset dimension %llu wasn't changed!\n", (unsigned long long) j);
                goto error;
            }
        }

        /*
         * Remember the current dimensionality of the dataset before
         * changing them again.
         */
        HDmemcpy(dims, new_dims, sizeof(new_dims));
    }


    if (H5Pclose(dcpl_id) < 0)
        TEST_ERROR
    if (H5Sclose(fspace_id) < 0)
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
        H5Pclose(dcpl_id);
        H5Sclose(fspace_id);
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
 * A test to check that a chunked dataset's extent can be
 * changed by using H5Dset_extent. This test uses fixed-size
 * dimensions for the dataset, so the dimensionality of the
 * dataset may only shrink.
 */
static int
test_dataset_set_extent_chunked_fixed(void)
{
    hsize_t dims[DATASET_SET_EXTENT_CHUNKED_FIXED_TEST_SPACE_RANK];
    hsize_t dims2[DATASET_SET_EXTENT_CHUNKED_FIXED_TEST_SPACE_RANK];
    hsize_t chunk_dims[DATASET_SET_EXTENT_CHUNKED_FIXED_TEST_SPACE_RANK];
    hsize_t new_dims[DATASET_SET_EXTENT_CHUNKED_FIXED_TEST_SPACE_RANK];
    size_t  i;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t   dset_id = H5I_INVALID_HID, dset_id2 = H5I_INVALID_HID;
    hid_t   dset_dtype = H5I_INVALID_HID;
    hid_t   dcpl_id = H5I_INVALID_HID;
    hid_t   fspace_id = H5I_INVALID_HID, fspace_id2 = H5I_INVALID_HID;

    TESTING("H5Dset_extent on chunked dataset with fixed dimensions")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_SET_EXTENT_CHUNKED_FIXED_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATASET_SET_EXTENT_CHUNKED_FIXED_TEST_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < DATASET_SET_EXTENT_CHUNKED_FIXED_TEST_SPACE_RANK; i++) {
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);
        dims2[i] = dims[i];
        do {
            chunk_dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);
        } while (chunk_dims[i] > dims[i]);
    }

    if ((fspace_id = H5Screate_simple(DATASET_SET_EXTENT_CHUNKED_FIXED_TEST_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR
    if ((fspace_id2 = H5Screate_simple(DATASET_SET_EXTENT_CHUNKED_FIXED_TEST_SPACE_RANK, dims2, NULL)) < 0)
        TEST_ERROR

    if ((dset_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((dcpl_id = H5Pcreate(H5P_DATASET_CREATE)) < 0)
        TEST_ERROR

    if (H5Pset_chunk(dcpl_id, DATASET_SET_EXTENT_CHUNKED_FIXED_TEST_SPACE_RANK, chunk_dims) < 0) {
        H5_FAILED();
        HDprintf("    unable to set dataset chunk dimensionality\n");
        goto error;
    }

    /*
     * NOTE: Since shrinking the dimension size can quickly end in a situation
     * where the dimensions are of size 1 and we can't shrink them further, we
     * use two datasets here to ensure the second test can run at least once.
     */
    if ((dset_id = H5Dcreate2(group_id, DATASET_SET_EXTENT_CHUNKED_FIXED_TEST_DSET_NAME, dset_dtype,
            fspace_id, H5P_DEFAULT, dcpl_id, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_SET_EXTENT_CHUNKED_FIXED_TEST_DSET_NAME);
        goto error;
    }

    if ((dset_id2 = H5Dcreate2(group_id, DATASET_SET_EXTENT_CHUNKED_FIXED_TEST_DSET_NAME2, dset_dtype,
            fspace_id2, H5P_DEFAULT, dcpl_id, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_SET_EXTENT_CHUNKED_FIXED_TEST_DSET_NAME2);
        goto error;
    }

    for (i = 0; i < DATASET_SET_EXTENT_CHUNKED_FIXED_TEST_NUM_PASSES; i++) {
        hbool_t skip_iterations = FALSE;
        size_t  j;

        for (j = 0; j < DATASET_SET_EXTENT_CHUNKED_FIXED_TEST_SPACE_RANK; j++) {
            /* Ensure that the new dimensionality is less than the old dimensionality. */
            do {
                if (dims[j] == 1) {
                    skip_iterations = TRUE;
                    break;
                }
                else
                    new_dims[j] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);
            } while (new_dims[j] >= dims[j]);
        }

        /*
         * If we've shrunk one of the dimensions to size 1, skip the rest of
         * the iterations.
         */
        if (skip_iterations)
            break;

        if (H5Dset_extent(dset_id, new_dims) < 0) {
            H5_FAILED();
            HDprintf("    failed to set dataset extent\n");
            goto error;
        }

        /* Retrieve the new dimensions of the dataset and ensure they
         * are different from the original.
         */
        if (H5Sclose(fspace_id) < 0)
            TEST_ERROR

        if ((fspace_id = H5Dget_space(dset_id)) < 0) {
            H5_FAILED();
            HDprintf("    failed to retrieve dataset's dataspace\n");
            goto error;
        }

        if (H5Sget_simple_extent_dims(fspace_id, new_dims, NULL) < 0) {
            H5_FAILED();
            HDprintf("    failed to retreive dataset dimensionality\n");
            goto error;
        }

        /*
         * Make sure the dimensions have been changed.
         */
        for (j = 0; j < DATASET_SET_EXTENT_CHUNKED_FIXED_TEST_SPACE_RANK; j++) {
            if (dims[j] == new_dims[j]) {
                H5_FAILED();
                HDprintf("    dataset dimension %llu wasn't changed!\n", (unsigned long long) j);
                goto error;
            }
        }

        /*
         * Remember the current dimensionality of the dataset before
         * changing them again.
         */
        HDmemcpy(dims, new_dims, sizeof(new_dims));
    }

    /*
     * Now close and re-open the dataset each pass to check the persistence
     * of the changes to the dataset's dimensionality.
     */
    for (i = 0; i < DATASET_SET_EXTENT_CHUNKED_FIXED_TEST_NUM_PASSES; i++) {
        hbool_t skip_iterations = FALSE;
        size_t  j;

        for (j = 0; j < DATASET_SET_EXTENT_CHUNKED_FIXED_TEST_SPACE_RANK; j++) {
            /* Ensure that the new dimensionality is less than the old dimensionality. */
            do {
                if (dims2[j] == 1) {
                    skip_iterations = TRUE;
                    break;
                }
                else
                    new_dims[j] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);
            } while (new_dims[j] >= dims2[j]);
        }

        /*
         * If we've shrunk one of the dimensions to size 1, skip the rest of
         * the iterations.
         */
        if (skip_iterations)
            break;

        if (H5Dset_extent(dset_id2, new_dims) < 0) {
            H5_FAILED();
            HDprintf("    failed to set dataset extent2\n");
            goto error;
        }

        /* Retrieve the new dimensions of the dataset and ensure they
         * are different from the original.
         */
        if (H5Sclose(fspace_id2) < 0)
            TEST_ERROR
        if (H5Dclose(dset_id2) < 0)
            TEST_ERROR

        if ((dset_id2 = H5Dopen2(group_id, DATASET_SET_EXTENT_CHUNKED_FIXED_TEST_DSET_NAME2, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDprintf("    failed to open dataset '%s'\n", DATASET_SET_EXTENT_CHUNKED_FIXED_TEST_DSET_NAME2);
            goto error;
        }

        if ((fspace_id2 = H5Dget_space(dset_id2)) < 0) {
            H5_FAILED();
            HDprintf("    failed to retrieve dataset's dataspace\n");
            goto error;
        }

        if (H5Sget_simple_extent_dims(fspace_id2, new_dims, NULL) < 0) {
            H5_FAILED();
            HDprintf("    failed to retreive dataset dimensionality\n");
            goto error;
        }

        /*
         * Make sure the dimensions have been changed.
         */
        for (j = 0; j < DATASET_SET_EXTENT_CHUNKED_FIXED_TEST_SPACE_RANK; j++) {
            if (dims2[j] == new_dims[j]) {
                H5_FAILED();
                HDprintf("    dataset dimension %llu wasn't changed!\n", (unsigned long long) j);
                goto error;
            }
        }

        /*
         * Remember the current dimensionality of the dataset before
         * changing them again.
         */
        HDmemcpy(dims2, new_dims, sizeof(new_dims));
    }


    if (H5Pclose(dcpl_id) < 0)
        TEST_ERROR
    if (H5Sclose(fspace_id) < 0)
        TEST_ERROR
    if (H5Sclose(fspace_id2) < 0)
        TEST_ERROR
    if (H5Tclose(dset_dtype) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id) < 0)
        TEST_ERROR
    if (H5Dclose(dset_id2) < 0)
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
        H5Pclose(dcpl_id);
        H5Sclose(fspace_id);
        H5Sclose(fspace_id2);
        H5Tclose(dset_dtype);
        H5Dclose(dset_id);
        H5Dclose(dset_id2);
        H5Gclose(group_id);
        H5Gclose(container_group);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

/*
 * A test to check that a dataset's extent can't be
 * changed when H5Dset_extent is passed invalid parameters.
 */
static int
test_dataset_set_extent_invalid_params(void)
{
    hsize_t dims[DATASET_SET_EXTENT_INVALID_PARAMS_TEST_SPACE_RANK];
    hsize_t chunk_dims[DATASET_SET_EXTENT_INVALID_PARAMS_TEST_SPACE_RANK];
    hsize_t new_dims[DATASET_SET_EXTENT_INVALID_PARAMS_TEST_SPACE_RANK];
    size_t  i;
    herr_t  err_ret = -1;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   container_group = H5I_INVALID_HID, group_id = H5I_INVALID_HID;
    hid_t   dset_id = H5I_INVALID_HID;
    hid_t   dset_dtype = H5I_INVALID_HID;
    hid_t   dcpl_id = H5I_INVALID_HID;
    hid_t   fspace_id = H5I_INVALID_HID;

    TESTING("H5Dset_extent with invalid parameters"); HDputs("");

    TESTING_2("H5Dset_extent with an invalid dataset ID")

    if ((fapl_id = h5_fileaccess()) < 0)
        TEST_ERROR

    if ((file_id = H5Fopen(vol_test_filename, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open file '%s'\n", vol_test_filename);
        goto error;
    }

    if ((container_group = H5Gopen2(file_id, DATASET_TEST_GROUP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't open container group '%s'\n", DATASET_TEST_GROUP_NAME);
        goto error;
    }

    if ((group_id = H5Gcreate2(container_group, DATASET_SET_EXTENT_INVALID_PARAMS_TEST_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create container sub-group '%s'\n", DATASET_SET_EXTENT_INVALID_PARAMS_TEST_GROUP_NAME);
        goto error;
    }

    for (i = 0; i < DATASET_SET_EXTENT_INVALID_PARAMS_TEST_SPACE_RANK; i++) {
        dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);
        do {
            new_dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);
        } while (new_dims[i] > dims[i]);
        do {
            chunk_dims[i] = (hsize_t) (rand() % MAX_DIM_SIZE + 1);
        } while (chunk_dims[i] > dims[i]);
    }

    if ((fspace_id = H5Screate_simple(DATASET_SET_EXTENT_INVALID_PARAMS_TEST_SPACE_RANK, dims, NULL)) < 0)
        TEST_ERROR

    if ((dset_dtype = generate_random_datatype(H5T_NO_CLASS)) < 0)
        TEST_ERROR

    if ((dcpl_id = H5Pcreate(H5P_DATASET_CREATE)) < 0)
        TEST_ERROR

    if (H5Pset_chunk(dcpl_id, DATASET_SET_EXTENT_INVALID_PARAMS_TEST_SPACE_RANK, chunk_dims) < 0) {
        H5_FAILED();
        HDprintf("    unable to set dataset chunk dimensionality\n");
        goto error;
    }

    if ((dset_id = H5Dcreate2(group_id, DATASET_SET_EXTENT_INVALID_PARAMS_TEST_DSET_NAME, dset_dtype,
            fspace_id, H5P_DEFAULT, dcpl_id, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDprintf("    couldn't create dataset '%s'\n", DATASET_SET_EXTENT_INVALID_PARAMS_TEST_DSET_NAME);
        goto error;
    }

    H5E_BEGIN_TRY {
        err_ret = H5Dset_extent(H5I_INVALID_HID, new_dims);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    setting dataset extent succeeded with an invalid dataset ID\n");
        goto error;
    }

    PASSED();

    TESTING_2("H5Dset_extent with NULL dimension pointer")

    H5E_BEGIN_TRY {
        err_ret = H5Dset_extent(dset_id, NULL);
    } H5E_END_TRY;

    if (err_ret >= 0) {
        H5_FAILED();
        HDprintf("    setting dataset extent succeeded with a NULL dimension pointer\n");
        goto error;
    }

    if (H5Pclose(dcpl_id) < 0)
        TEST_ERROR
    if (H5Sclose(fspace_id) < 0)
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
        H5Pclose(dcpl_id);
        H5Sclose(fspace_id);
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
 * A test for H5Dflush.
 */
static int
test_flush_dataset(void)
{
    TESTING("H5Dflush")

    SKIPPED();

    return 0;
}

/*
 * A test to check that H5Dflush fails when it is
 * passed invalid parameters.
 */
static int
test_flush_dataset_invalid_params(void)
{
    TESTING("H5Dflush with invalid parameters")

    SKIPPED();

    return 0;
}

/*
 * A test for H5Drefresh.
 */
static int
test_refresh_dataset(void)
{
    TESTING("H5Drefresh")

    SKIPPED();

    return 0;
}

/*
 * A test to check that H5Drefresh fails when it is
 * passed invalid parameters.
 */
static int
test_refresh_dataset_invalid_params(void)
{
    TESTING("H5Drefresh")

    SKIPPED();

    return 0;
}

int
vol_dataset_test(void)
{
    size_t i;
    int    nerrors;

    HDprintf("**********************************************\n");
    HDprintf("*                                            *\n");
    HDprintf("*             VOL Dataset Tests              *\n");
    HDprintf("*                                            *\n");
    HDprintf("**********************************************\n\n");

    for (i = 0, nerrors = 0; i < ARRAY_LENGTH(dataset_tests); i++) {
        nerrors += (*dataset_tests[i])() ? 1 : 0;
    }

    HDprintf("\n");

done:
    return nerrors;
}
