/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

/**
 * Purpose: Tests H5M (Map API) interface in parallel.
 */

#include "h5daos_test.h"

#include "daos_vol.h"

#define PARALLEL_FILENAME "h5daos_test_map_parallel.h5"

/*
 * Currently unsupported functionality
 */
#define NO_GET_MAP_COUNT

/*
 * Global variables
 */
uuid_t pool_uuid;
int    mpi_rank;
int    mpi_size;

/*
 * Local prototypes
 */
static herr_t iterate_func1(hid_t map_id, const void *key, void *op_data);
static herr_t iterate_func2(hid_t map_id, const void *key, void *op_data);
static herr_t iterate_func3(hid_t map_id, const void *key, void *op_data);
static herr_t iterate_func4(hid_t map_id, const void *key, void *op_data);

typedef struct delete_rank_0_test_info {
    size_t  key_count;
    hbool_t second_pass;
} delete_rank_0_test_info;

typedef struct delete_all_ranks_test_info {
    size_t key_count;
    size_t pass_number;
} delete_all_ranks_test_info;

/*
 * A test to simply create a map on rank 0 only and then
 * ensure that all ranks can re-open the map.
 */
#define MAP_TEST_CREATE_RANK_0_ONLY_MAP_NAME "create_rank_0_only_map"
#define MAP_TEST_CREATE_RANK_0_ONLY_KEY_TYPE H5T_NATIVE_INT
#define MAP_TEST_CREATE_RANK_0_ONLY_VAL_TYPE H5T_NATIVE_INT
static int
test_create_map_rank_0()
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t map_id = H5I_INVALID_HID;

    TESTING_2("creation of map object on rank 0 only - reopen on all ranks");

    if ((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0) {
        H5_FAILED();
        HDputs("    failed to create FAPL");
        goto error;
    }

    if (H5Pset_all_coll_metadata_ops(fapl_id, 1) < 0) {
        H5_FAILED();
        HDputs("    failed to set collective metadata reads");
        goto error;
    }

    if (MAINPROCESS) {
        if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDWR, fapl_id)) < 0) {
            H5_FAILED();
            HDputs("    failed to open file");
            goto error;
        }

        if ((map_id = H5Mcreate(file_id, MAP_TEST_CREATE_RANK_0_ONLY_MAP_NAME,
                                MAP_TEST_CREATE_RANK_0_ONLY_KEY_TYPE, MAP_TEST_CREATE_RANK_0_ONLY_VAL_TYPE,
                                H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDputs("    failed to create map");
            goto error;
        }

        if (H5Mclose(map_id) < 0) {
            H5_FAILED();
            HDputs("    failed to close map");
            goto error;
        }

        if (H5Fclose(file_id) < 0) {
            H5_FAILED();
            HDputs("    failed to close file");
            goto error;
        }
    }

    /*
     * Re-open the file and map object on all ranks.
     */
    if (H5Pset_fapl_mpio(fapl_id, MPI_COMM_WORLD, MPI_INFO_NULL) < 0) {
        H5_FAILED();
        HDputs("    failed to set MPI on FAPL");
        goto error;
    }

    if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDONLY, fapl_id)) < 0) {
        H5_FAILED();
        HDputs("    failed to open file");
        goto error;
    }

    if ((map_id = H5Mopen(file_id, MAP_TEST_CREATE_RANK_0_ONLY_MAP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDputs("    failed to open map");
        goto error;
    }

    if (H5Mclose(map_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close map");
        goto error;
    }

    if (H5Pclose(fapl_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close FAPL");
        goto error;
    }

    if (H5Fclose(file_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close file");
        goto error;
    }

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Mclose(map_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    }
    H5E_END_TRY;

    return 1;
}

/*
 * A test to simply create a map on all ranks and then
 * ensure that all ranks can re-open the map.
 */
#define MAP_TEST_CREATE_ALL_RANKS_MAP_NAME "create_all_ranks_map"
#define MAP_TEST_CREATE_ALL_RANKS_KEY_TYPE H5T_NATIVE_INT
#define MAP_TEST_CREATE_ALL_RANKS_VAL_TYPE H5T_NATIVE_INT
static int
test_create_map_all_ranks()
{
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t map_id = H5I_INVALID_HID;

    TESTING_2("creation of map object on all ranks - reopen on all ranks");

    if ((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0) {
        H5_FAILED();
        HDputs("    failed to create FAPL");
        goto error;
    }

    if (H5Pset_all_coll_metadata_ops(fapl_id, 1) < 0) {
        H5_FAILED();
        HDputs("    failed to set collective metadata reads");
        goto error;
    }

    if (H5Pset_fapl_mpio(fapl_id, MPI_COMM_WORLD, MPI_INFO_NULL) < 0) {
        H5_FAILED();
        HDputs("    failed to set MPI on FAPL");
        goto error;
    }

    if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDputs("    failed to open file");
        goto error;
    }

    if ((map_id = H5Mcreate(file_id, MAP_TEST_CREATE_ALL_RANKS_MAP_NAME, MAP_TEST_CREATE_ALL_RANKS_KEY_TYPE,
                            MAP_TEST_CREATE_ALL_RANKS_VAL_TYPE, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDputs("    failed to create map");
        goto error;
    }

    if (H5Mclose(map_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close map");
        goto error;
    }

    if (H5Fclose(file_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close file");
        goto error;
    }

    if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDONLY, fapl_id)) < 0) {
        H5_FAILED();
        HDputs("    failed to open file");
        goto error;
    }

    if ((map_id = H5Mopen(file_id, MAP_TEST_CREATE_ALL_RANKS_MAP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDputs("    failed to open map");
        goto error;
    }

    if (H5Mclose(map_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close map");
        goto error;
    }

    if (H5Pclose(fapl_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close FAPL");
        goto error;
    }

    if (H5Fclose(file_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close file");
        goto error;
    }

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Mclose(map_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    }
    H5E_END_TRY;

    return 1;
}

/*
 * A test to create a map on rank 0, insert some keys and then
 * re-open the map on all ranks and ensure that the keys exist.
 */
#define MAP_TEST_INSERT_RANK_0_ONLY_MAP_NAME   "insert_rank_0_only_map"
#define MAP_TEST_INSERT_RANK_0_ONLY_KEY_TYPE   H5T_NATIVE_INT
#define MAP_TEST_INSERT_RANK_0_ONLY_VAL_TYPE   H5T_NATIVE_INT
#define MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE int
#define MAP_TEST_INSERT_RANK_0_ONLY_VAL_C_TYPE int
static int
test_insert_keys_one_rank()
{
    MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE key1, key2, key3;
    MAP_TEST_INSERT_RANK_0_ONLY_VAL_C_TYPE val1, val2, val3;
    hid_t                                  file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t                                  map_id = H5I_INVALID_HID;

    TESTING_2("inserting keys on rank 0 then retrieving values on all ranks");

    key1 = 0;
    key2 = 1;
    key3 = 2;

    if ((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0) {
        H5_FAILED();
        HDputs("    failed to create FAPL");
        goto error;
    }

    if (H5Pset_all_coll_metadata_ops(fapl_id, 1) < 0) {
        H5_FAILED();
        HDputs("    failed to set collective metadata reads");
        goto error;
    }

    if (MAINPROCESS) {
        val1 = 10;
        val2 = 20;
        val3 = 30;

        if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDWR, fapl_id)) < 0) {
            H5_FAILED();
            HDputs("    failed to open file");
            goto error;
        }

        if ((map_id = H5Mcreate(file_id, MAP_TEST_INSERT_RANK_0_ONLY_MAP_NAME,
                                MAP_TEST_INSERT_RANK_0_ONLY_KEY_TYPE, MAP_TEST_INSERT_RANK_0_ONLY_VAL_TYPE,
                                H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDputs("    failed to create map");
            goto error;
        }

        /*
         * Insert a few keys
         */
        if (H5Mput(map_id, MAP_TEST_INSERT_RANK_0_ONLY_KEY_TYPE, &key1, MAP_TEST_INSERT_RANK_0_ONLY_VAL_TYPE,
                   &val1, H5P_DEFAULT)) {
            H5_FAILED();
            HDputs("    failed to set key-value pair in map");
            goto error;
        }

        if (H5Mput(map_id, MAP_TEST_INSERT_RANK_0_ONLY_KEY_TYPE, &key2, MAP_TEST_INSERT_RANK_0_ONLY_VAL_TYPE,
                   &val2, H5P_DEFAULT)) {
            H5_FAILED();
            HDputs("    failed to set key-value pair in map");
            goto error;
        }

        if (H5Mput(map_id, MAP_TEST_INSERT_RANK_0_ONLY_KEY_TYPE, &key3, MAP_TEST_INSERT_RANK_0_ONLY_VAL_TYPE,
                   &val3, H5P_DEFAULT)) {
            H5_FAILED();
            HDputs("    failed to set key-value pair in map");
            goto error;
        }

        if (H5Mclose(map_id) < 0) {
            H5_FAILED();
            HDputs("    failed to close map");
            goto error;
        }

        if (H5Fclose(file_id) < 0) {
            H5_FAILED();
            HDputs("    failed to close file");
            goto error;
        }
    }

    /*
     * Re-open the file and map object on all ranks.
     */
    if (H5Pset_fapl_mpio(fapl_id, MPI_COMM_WORLD, MPI_INFO_NULL) < 0) {
        H5_FAILED();
        HDputs("    failed to set MPI on FAPL");
        goto error;
    }

    if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDONLY, fapl_id)) < 0) {
        H5_FAILED();
        HDputs("    failed to open file");
        goto error;
    }

    if ((map_id = H5Mopen(file_id, MAP_TEST_INSERT_RANK_0_ONLY_MAP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDputs("    failed to open map");
        goto error;
    }

    /*
     * Read the keys from the map object on all ranks.
     */
    val1 = -1;
    val2 = -1;
    val3 = -1;
    if (H5Mget(map_id, MAP_TEST_INSERT_RANK_0_ONLY_KEY_TYPE, &key1, MAP_TEST_INSERT_RANK_0_ONLY_VAL_TYPE,
               &val1, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDputs("    failed to retrieve value by key from map");
        goto error;
    }

    if (val1 != 10) {
        H5_FAILED();
        HDputs("    value for first key did not match expected value");
        goto error;
    }

    if (H5Mget(map_id, MAP_TEST_INSERT_RANK_0_ONLY_KEY_TYPE, &key2, MAP_TEST_INSERT_RANK_0_ONLY_VAL_TYPE,
               &val2, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDputs("    failed to retrieve value by key from map");
        goto error;
    }

    if (val2 != 20) {
        H5_FAILED();
        HDputs("    value for second key did not match expected value");
        goto error;
    }

    if (H5Mget(map_id, MAP_TEST_INSERT_RANK_0_ONLY_KEY_TYPE, &key3, MAP_TEST_INSERT_RANK_0_ONLY_VAL_TYPE,
               &val3, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDputs("    failed to retrieve value by key from map");
        goto error;
    }

    if (val3 != 30) {
        H5_FAILED();
        HDputs("    value for third key did not match expected value");
        goto error;
    }

    if (H5Mclose(map_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close map");
        goto error;
    }

    if (H5Pclose(fapl_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close FAPL");
        goto error;
    }

    if (H5Fclose(file_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close file");
        goto error;
    }

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Mclose(map_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    }
    H5E_END_TRY;

    return 1;
}

/*
 * A test to create a map on all ranks, insert some different keys
 * on all ranks and then re-open the map on all ranks and ensure
 * that every key exists.
 */
#define MAP_TEST_INSERT_ALL_RANKS_N_KEYS_PER_RANK 3
#define MAP_TEST_INSERT_ALL_RANKS_MAP_NAME        "insert_all_ranks_map"
#define MAP_TEST_INSERT_ALL_RANKS_KEY_TYPE        H5T_NATIVE_INT
#define MAP_TEST_INSERT_ALL_RANKS_VAL_TYPE        H5T_NATIVE_INT
#define MAP_TEST_INSERT_ALL_RANKS_KEY_C_TYPE      int
#define MAP_TEST_INSERT_ALL_RANKS_VAL_C_TYPE      int
static int
test_insert_keys_all_ranks()
{
    MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE *keys           = NULL;
    MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE *retrieved_keys = NULL;
    MAP_TEST_INSERT_RANK_0_ONLY_VAL_C_TYPE *vals           = NULL;
    MAP_TEST_INSERT_RANK_0_ONLY_VAL_C_TYPE *retrieved_vals = NULL;
#ifndef NO_GET_MAP_COUNT
    hsize_t key_count;
#endif
    size_t i;
    hid_t  file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t  map_id = H5I_INVALID_HID;

    TESTING_2("inserting different keys on all ranks then retrieving values on all ranks");

    if (NULL ==
        (keys = (MAP_TEST_INSERT_ALL_RANKS_KEY_C_TYPE *)malloc(
             MAP_TEST_INSERT_ALL_RANKS_N_KEYS_PER_RANK * sizeof(MAP_TEST_INSERT_ALL_RANKS_KEY_C_TYPE)))) {
        H5_FAILED();
        HDputs("    failed to allocate buffer for map keys");
        goto error;
    }

    if (NULL ==
        (vals = (MAP_TEST_INSERT_ALL_RANKS_VAL_C_TYPE *)malloc(
             MAP_TEST_INSERT_ALL_RANKS_N_KEYS_PER_RANK * sizeof(MAP_TEST_INSERT_ALL_RANKS_VAL_C_TYPE)))) {
        H5_FAILED();
        HDputs("    failed to allocate buffer for map values");
        goto error;
    }

    if ((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0) {
        H5_FAILED();
        HDputs("    failed to create FAPL");
        goto error;
    }

    if (H5Pset_all_coll_metadata_ops(fapl_id, 1) < 0) {
        H5_FAILED();
        HDputs("    failed to set collective metadata reads");
        goto error;
    }

    if (H5Pset_fapl_mpio(fapl_id, MPI_COMM_WORLD, MPI_INFO_NULL) < 0) {
        H5_FAILED();
        HDputs("    failed to set MPI on FAPL");
        goto error;
    }

    if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDputs("    failed to open file");
        goto error;
    }

    if ((map_id = H5Mcreate(file_id, MAP_TEST_INSERT_ALL_RANKS_MAP_NAME, MAP_TEST_INSERT_ALL_RANKS_KEY_TYPE,
                            MAP_TEST_INSERT_ALL_RANKS_VAL_TYPE, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDputs("    failed to create map");
        goto error;
    }

    for (i = 0; i < MAP_TEST_INSERT_ALL_RANKS_N_KEYS_PER_RANK; i++) {
        /* Keys range from 0 to ((mpi_size * MAP_TEST_INSERT_ALL_RANKS_N_KEYS_PER_RANK) - 1) */
        keys[i] =
            (MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE)((size_t)(mpi_rank *
                                                              MAP_TEST_INSERT_ALL_RANKS_N_KEYS_PER_RANK) +
                                                     i);
        /* Values simply determined by the current rank number */
        vals[i] = mpi_rank;

        if (H5Mput(map_id, MAP_TEST_INSERT_ALL_RANKS_KEY_TYPE, &(keys[i]), MAP_TEST_INSERT_ALL_RANKS_VAL_TYPE,
                   &(vals[i]), H5P_DEFAULT) < 0) {
            H5_FAILED();
            HDputs("    failed to set key-value pair in map");
            goto error;
        }
    }

    if (H5Mclose(map_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close map");
        goto error;
    }

    if (H5Fclose(file_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close file");
        goto error;
    }

    if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDONLY, fapl_id)) < 0) {
        H5_FAILED();
        HDputs("    failed to open file");
        goto error;
    }

    if ((map_id = H5Mopen(file_id, MAP_TEST_INSERT_ALL_RANKS_MAP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDputs("    failed to open map");
        goto error;
    }

#ifndef NO_GET_MAP_COUNT
    /*
     * Make sure the map key count matches what is expected.
     */
    if (H5Mget_count(map_id, &key_count, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDputs("    failed to retrieve map's key count");
        goto error;
    }

    if (key_count != (hsize_t)(mpi_size * MAP_TEST_INSERT_ALL_RANKS_N_KEYS_PER_RANK)) {
        H5_FAILED();
        printf("    map's key count of %lld did not match expected value %lld", (long long)key_count,
               (long long)(mpi_size * MAP_TEST_INSERT_ALL_RANKS_N_KEYS_PER_RANK));
        goto error;
    }
#endif

    if (NULL == (retrieved_keys = (MAP_TEST_INSERT_ALL_RANKS_KEY_C_TYPE *)malloc(
                     (size_t)(MAP_TEST_INSERT_ALL_RANKS_N_KEYS_PER_RANK * mpi_size) *
                     sizeof(MAP_TEST_INSERT_ALL_RANKS_KEY_C_TYPE)))) {
        H5_FAILED();
        HDputs("    failed to allocate buffer for retrieved map keys");
        goto error;
    }

    if (NULL == (retrieved_vals = (MAP_TEST_INSERT_ALL_RANKS_VAL_C_TYPE *)malloc(
                     (size_t)(MAP_TEST_INSERT_ALL_RANKS_N_KEYS_PER_RANK * mpi_size) *
                     sizeof(MAP_TEST_INSERT_ALL_RANKS_VAL_C_TYPE)))) {
        H5_FAILED();
        HDputs("    failed to allocate buffer for retrieved map values");
        goto error;
    }

    for (i = 0; i < (size_t)mpi_size * MAP_TEST_INSERT_ALL_RANKS_N_KEYS_PER_RANK; i++) {
        retrieved_keys[i] = (MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE)i;
        retrieved_vals[i] = -1;

        if (H5Mget(map_id, MAP_TEST_INSERT_ALL_RANKS_KEY_TYPE, &(retrieved_keys[i]),
                   MAP_TEST_INSERT_ALL_RANKS_VAL_TYPE, &(retrieved_vals[i]), H5P_DEFAULT) < 0) {
            H5_FAILED();
            HDputs("    failed to retrieve value by key from map");
            goto error;
        }

        if (retrieved_vals[i] !=
            (MAP_TEST_INSERT_ALL_RANKS_KEY_C_TYPE)(i / MAP_TEST_INSERT_ALL_RANKS_N_KEYS_PER_RANK)) {
            H5_FAILED();
            printf("    value for key %lld did not match expected value\n", (long long)i);
            goto error;
        }
    }

    free(keys);
    keys = NULL;
    free(retrieved_keys);
    retrieved_keys = NULL;
    free(vals);
    vals = NULL;
    free(retrieved_vals);
    retrieved_vals = NULL;

    if (H5Mclose(map_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close map");
        goto error;
    }

    if (H5Pclose(fapl_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close FAPL");
        goto error;
    }

    if (H5Fclose(file_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close file");
        goto error;
    }

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY
    {
        if (keys)
            free(keys);
        if (retrieved_keys)
            free(retrieved_keys);
        if (vals)
            free(vals);
        if (retrieved_vals)
            free(retrieved_vals);
        H5Mclose(map_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    }
    H5E_END_TRY;

    return 1;
}

/*
 * A test to create a map on all ranks, insert some keys on rank 0
 * and then have all ranks iterate over the keys in the map.
 */
#define MAP_TEST_INSERT_RANK_0_ITERATE_ALL_RANKS_MAP_NAME   "insert_rank_0_iterate_all_ranks_map"
#define MAP_TEST_INSERT_RANK_0_ITERATE_ALL_RANKS_KEY_TYPE   H5T_NATIVE_INT
#define MAP_TEST_INSERT_RANK_0_ITERATE_ALL_RANKS_VAL_TYPE   H5T_NATIVE_INT
#define MAP_TEST_INSERT_RANK_0_ITERATE_ALL_RANKS_KEY_C_TYPE int
#define MAP_TEST_INSERT_RANK_0_ITERATE_ALL_RANKS_VAL_C_TYPE int
#define MAP_TEST_INSERT_RANK_0_ITERATE_ALL_RANKS_N_KEYS     10
static int
test_insert_keys_one_rank_iterate_all_ranks()
{
    size_t key_count;
    hid_t  file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t  map_id = H5I_INVALID_HID;

    TESTING_2("insert keys on rank 0 - iterate over keys on all ranks");

    if ((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0) {
        H5_FAILED();
        HDputs("    failed to create FAPL");
        goto error;
    }

    if (H5Pset_all_coll_metadata_ops(fapl_id, 1) < 0) {
        H5_FAILED();
        HDputs("    failed to set collective metadata reads");
        goto error;
    }

    if (H5Pset_fapl_mpio(fapl_id, MPI_COMM_WORLD, MPI_INFO_NULL) < 0) {
        H5_FAILED();
        HDputs("    failed to set MPI on FAPL");
        goto error;
    }

    if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDputs("    failed to open file");
        goto error;
    }

    if ((map_id = H5Mcreate(file_id, MAP_TEST_INSERT_RANK_0_ITERATE_ALL_RANKS_MAP_NAME,
                            MAP_TEST_INSERT_RANK_0_ITERATE_ALL_RANKS_KEY_TYPE,
                            MAP_TEST_INSERT_RANK_0_ITERATE_ALL_RANKS_VAL_TYPE, H5P_DEFAULT, H5P_DEFAULT,
                            H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDputs("    failed to create map");
        goto error;
    }

    /*
     * Insert some keys on rank 0 only.
     */
    if (MAINPROCESS) {
        MAP_TEST_INSERT_RANK_0_ITERATE_ALL_RANKS_KEY_C_TYPE cur_key;
        MAP_TEST_INSERT_RANK_0_ITERATE_ALL_RANKS_VAL_C_TYPE cur_val;
        size_t                                              i;

        for (i = 0; i < MAP_TEST_INSERT_RANK_0_ITERATE_ALL_RANKS_N_KEYS; i++) {
            /* Keys range from 0 to (MAP_TEST_INSERT_RANK_0_ITERATE_ALL_RANKS_N_KEYS - 1) */
            cur_key = (MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE)i;
            /* Values simply determined by 10 times the current iteration number */
            cur_val = (MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE)(i * 10);

            if (H5Mput(map_id, MAP_TEST_INSERT_RANK_0_ITERATE_ALL_RANKS_KEY_TYPE, &cur_key,
                       MAP_TEST_INSERT_RANK_0_ITERATE_ALL_RANKS_VAL_TYPE, &cur_val, H5P_DEFAULT) < 0) {
                H5_FAILED();
                printf("    failed to set key-value pair %lld in map\n", (long long)i);
                goto error;
            }
        }
    }

    /*
     * Re-open the map to ensure the keys make it.
     */
    if (H5Mclose(map_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close map");
        goto error;
    }

    if (H5Fclose(file_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close file");
        goto error;
    }

    if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDputs("    failed to open file");
        goto error;
    }

    if ((map_id = H5Mopen(file_id, MAP_TEST_INSERT_RANK_0_ITERATE_ALL_RANKS_MAP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDputs("    failed to open map");
        goto error;
    }

    /*
     * Iterate over all keys, making sure that the count matches what is
     * expected and that each key is a multiple of 10.
     */
    key_count = 0;
    if (H5Miterate(map_id, NULL, MAP_TEST_INSERT_RANK_0_ITERATE_ALL_RANKS_KEY_TYPE, iterate_func1, &key_count,
                   H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDputs("    iteration over keys in map failed");
        goto error;
    }

    if (key_count != (size_t)MAP_TEST_INSERT_RANK_0_ITERATE_ALL_RANKS_N_KEYS) {
        H5_FAILED();
        printf("    after iteration, key count of %lld did not match expected value %lld\n",
               (long long)key_count, (long long)MAP_TEST_INSERT_RANK_0_ITERATE_ALL_RANKS_N_KEYS);
        goto error;
    }

    if (H5Mclose(map_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close map");
        goto error;
    }

    if (H5Pclose(fapl_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close FAPL");
        goto error;
    }

    if (H5Fclose(file_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close file");
        goto error;
    }

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Mclose(map_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    }
    H5E_END_TRY;

    return 1;
}

/*
 * A test to create a map on all ranks, insert some keys on all ranks
 * and then have all ranks iterate over the keys in the map.
 */
#define MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_MAP_NAME   "insert_all_ranks_iterate_all_ranks_map"
#define MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_KEY_TYPE   H5T_NATIVE_INT
#define MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_VAL_TYPE   H5T_NATIVE_INT
#define MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_KEY_C_TYPE int
#define MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_VAL_C_TYPE int
#define MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_N_KEYS     10
static int
test_insert_keys_all_ranks_iterate_all_ranks()
{
    MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_KEY_C_TYPE cur_key;
    MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_VAL_C_TYPE cur_val;
    size_t                                                 i, key_count;
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t map_id = H5I_INVALID_HID;

    TESTING_2("insert keys on all ranks - iterate over keys on all ranks");

    if ((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0) {
        H5_FAILED();
        HDputs("    failed to create FAPL");
        goto error;
    }

    if (H5Pset_all_coll_metadata_ops(fapl_id, 1) < 0) {
        H5_FAILED();
        HDputs("    failed to set collective metadata reads");
        goto error;
    }

    if (H5Pset_fapl_mpio(fapl_id, MPI_COMM_WORLD, MPI_INFO_NULL) < 0) {
        H5_FAILED();
        HDputs("    failed to set MPI on FAPL");
        goto error;
    }

    if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDputs("    failed to open file");
        goto error;
    }

    if ((map_id = H5Mcreate(file_id, MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_MAP_NAME,
                            MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_KEY_TYPE,
                            MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_VAL_TYPE, H5P_DEFAULT, H5P_DEFAULT,
                            H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDputs("    failed to create map");
        goto error;
    }

    /*
     * Insert some keys on all ranks.
     */
    for (i = 0; i < MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_N_KEYS; i++) {
        /* Keys range from 0 to ((mpi_size * MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_N_KEYS) - 1) */
        cur_key =
            (MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE)((size_t)(mpi_rank *
                                                              MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_N_KEYS) +
                                                     i);
        /* Values simply determined by 5 times the current rank number */
        cur_val = mpi_rank * 5;

        if (H5Mput(map_id, MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_KEY_TYPE, &cur_key,
                   MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_VAL_TYPE, &cur_val, H5P_DEFAULT) < 0) {
            H5_FAILED();
            printf("    failed to set key-value pair %lld in map\n", (long long)i);
            goto error;
        }
    }

    /*
     * Re-open the map to ensure the keys make it.
     */
    if (H5Mclose(map_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close map");
        goto error;
    }

    if (H5Fclose(file_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close file");
        goto error;
    }

    if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDputs("    failed to open file");
        goto error;
    }

    if ((map_id = H5Mopen(file_id, MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_MAP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDputs("    failed to open map");
        goto error;
    }

    /*
     * Iterate over all keys, making sure that the count matches what is
     * expected and that each key is a multiple of 5.
     */
    key_count = 0;
    if (H5Miterate(map_id, NULL, MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_KEY_TYPE, iterate_func2,
                   &key_count, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDputs("    iteration over keys in map failed");
        goto error;
    }

    if (key_count != (size_t)(mpi_size * MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_N_KEYS)) {
        H5_FAILED();
        printf("    after iteration, key count of %lld did not match expected value %lld\n",
               (long long)key_count,
               (long long)(mpi_size * MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_N_KEYS));
        goto error;
    }

    if (H5Mclose(map_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close map");
        goto error;
    }

    if (H5Pclose(fapl_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close FAPL");
        goto error;
    }

    if (H5Fclose(file_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close file");
        goto error;
    }

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Mclose(map_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    }
    H5E_END_TRY;

    return 1;
}

/*
 * A test to create a map on all ranks and insert some keys on
 * all ranks. After this, rank 0 deletes a few keys and all of
 * the ranks then iterate over the remaining keys to check that
 * the keys were actually deleted.
 */
#define MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_MAP_NAME   "delete_rank_0_iterate_all_ranks_map"
#define MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_KEY_TYPE   H5T_NATIVE_INT
#define MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_VAL_TYPE   H5T_NATIVE_INT
#define MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_KEY_C_TYPE int
#define MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_VAL_C_TYPE int
#define MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_N_KEYS     10
static int
test_delete_keys_one_rank_iterate_all_ranks()
{
    MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_KEY_C_TYPE cur_key;
    MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_VAL_C_TYPE cur_val;
    delete_rank_0_test_info                             test_info;
    size_t                                              i;
    hid_t                                               file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t                                               map_id = H5I_INVALID_HID;

    TESTING_2("delete keys on rank 0 - iterate over keys on all ranks");

    if ((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0) {
        H5_FAILED();
        HDputs("    failed to create FAPL");
        goto error;
    }

    if (H5Pset_all_coll_metadata_ops(fapl_id, 1) < 0) {
        H5_FAILED();
        HDputs("    failed to set collective metadata reads");
        goto error;
    }

    if (H5Pset_fapl_mpio(fapl_id, MPI_COMM_WORLD, MPI_INFO_NULL) < 0) {
        H5_FAILED();
        HDputs("    failed to set MPI on FAPL");
        goto error;
    }

    if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDputs("    failed to open file");
        goto error;
    }

    if ((map_id = H5Mcreate(file_id, MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_MAP_NAME,
                            MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_KEY_TYPE,
                            MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_VAL_TYPE, H5P_DEFAULT, H5P_DEFAULT,
                            H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDputs("    failed to create map");
        goto error;
    }

    /*
     * Insert some keys on all ranks.
     */
    for (i = 0; i < MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_N_KEYS; i++) {
        /* Keys range from 0 to ((mpi_size * MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_N_KEYS) - 1) */
        cur_key =
            (MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE)((size_t)(mpi_rank *
                                                              MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_N_KEYS) +
                                                     i);
        /* Values simply determined by 5 times the current rank number */
        cur_val = mpi_rank * 5;

        if (H5Mput(map_id, MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_KEY_TYPE, &cur_key,
                   MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_VAL_TYPE, &cur_val, H5P_DEFAULT) < 0) {
            H5_FAILED();
            printf("    failed to set key-value pair %lld in map\n", (long long)i);
            goto error;
        }
    }

    /*
     * Re-open the map to ensure the keys make it.
     */
    if (H5Mclose(map_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close map");
        goto error;
    }

    if (H5Fclose(file_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close file");
        goto error;
    }

    if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDputs("    failed to open file");
        goto error;
    }

    if ((map_id = H5Mopen(file_id, MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_MAP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDputs("    failed to open map");
        goto error;
    }

    /*
     * Iterate over all keys, making sure that the count matches what is
     * expected and that each key is a multiple of 5.
     */
    test_info.key_count   = 0;
    test_info.second_pass = 0;
    if (H5Miterate(map_id, NULL, MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_KEY_TYPE, iterate_func3, &test_info,
                   H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDputs("    iteration over keys in map failed");
        goto error;
    }

    if (test_info.key_count != (size_t)(mpi_size * MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_N_KEYS)) {
        H5_FAILED();
        printf("    after first iteration, key count of %lld did not match expected value %lld\n",
               (long long)test_info.key_count,
               (long long)(mpi_size * MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_N_KEYS));
        goto error;
    }

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        HDputs("    MPI_Barrier failed");
        goto error;
    }

    /*
     * Have rank 0 delete all of its own keys, then have all ranks iterate
     * over the keys again to make sure they are gone.
     */
    if (MAINPROCESS) {
        for (i = 0; i < MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_N_KEYS; i++) {
            cur_key =
                (MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE)((size_t)(mpi_rank *
                                                                  MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_N_KEYS) +
                                                         i);

            if (H5Mdelete(map_id, MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_KEY_TYPE, &cur_key, H5P_DEFAULT) <
                0) {
                H5_FAILED();
                printf("    failed to delete key %lld in map\n", (long long)i);
                goto error;
            }
        }
    }

    /*
     * Re-open the map to ensure the keys make it.
     */
    if (H5Mclose(map_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close map");
        goto error;
    }

    if (H5Fclose(file_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close file");
        goto error;
    }

    if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDputs("    failed to open file");
        goto error;
    }

    if ((map_id = H5Mopen(file_id, MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_MAP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDputs("    failed to open map");
        goto error;
    }

    test_info.key_count   = 0;
    test_info.second_pass = 1;
    if (H5Miterate(map_id, NULL, MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_KEY_TYPE, iterate_func3, &test_info,
                   H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDputs("    iteration over keys in map failed");
        goto error;
    }

    if (test_info.key_count != (size_t)((mpi_size * MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_N_KEYS) -
                                        MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_N_KEYS)) {
        H5_FAILED();
        printf("    after second iteration, key count of %lld did not match expected value %lld\n",
               (long long)test_info.key_count,
               (long long)((mpi_size * MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_N_KEYS) -
                           MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_N_KEYS));
        goto error;
    }

    if (H5Mclose(map_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close map");
        goto error;
    }

    if (H5Pclose(fapl_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close FAPL");
        goto error;
    }

    if (H5Fclose(file_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close file");
        goto error;
    }

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Mclose(map_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    }
    H5E_END_TRY;

    return 1;
}

/*
 * A test to create a map on all ranks and insert some keys on
 * all ranks. After this, each rank deletes one of its keys and
 * all of the ranks then iterate over the remaining keys to check
 * that the keys were actually deleted.
 *
 * NOTE: this test requires support for independent metadata writes.
 */
#define MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_MAP_NAME   "delete_all_ranks_iterate_all_ranks_map"
#define MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_KEY_TYPE   H5T_NATIVE_INT
#define MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_VAL_TYPE   H5T_NATIVE_INT
#define MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_KEY_C_TYPE int
#define MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_VAL_C_TYPE int
#define MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_N_KEYS     10
static int
test_delete_keys_all_ranks_iterate_all_ranks()
{
    MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_KEY_C_TYPE cur_key;
    MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_VAL_C_TYPE cur_val;
    delete_all_ranks_test_info                             test_info;
    size_t                                                 i;
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t map_id = H5I_INVALID_HID;

    TESTING_2("delete keys on all ranks - iterate over keys on all ranks");

    if ((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0) {
        H5_FAILED();
        HDputs("    failed to create FAPL");
        goto error;
    }

    if (H5Pset_all_coll_metadata_ops(fapl_id, 1) < 0) {
        H5_FAILED();
        HDputs("    failed to set collective metadata reads");
        goto error;
    }

    if (H5Pset_fapl_mpio(fapl_id, MPI_COMM_WORLD, MPI_INFO_NULL) < 0) {
        H5_FAILED();
        HDputs("    failed to set MPI on FAPL");
        goto error;
    }

    if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDputs("    failed to open file");
        goto error;
    }

    if ((map_id = H5Mcreate(file_id, MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_MAP_NAME,
                            MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_KEY_TYPE,
                            MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_VAL_TYPE, H5P_DEFAULT, H5P_DEFAULT,
                            H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDputs("    failed to create map");
        goto error;
    }

    /*
     * Insert some keys on all ranks.
     */
    for (i = 0; i < MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_N_KEYS; i++) {
        /* Keys range from 0 to ((mpi_size * MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_N_KEYS) - 1) */
        cur_key =
            (MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE)((size_t)(mpi_rank *
                                                              MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_N_KEYS) +
                                                     i);
        /* Values simply determined by the current iteration number. Therefore each rank has N keys
         * with values ranging from 0 to (N keys - 1) */
        cur_val = (MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE)i;

        if (H5Mput(map_id, MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_KEY_TYPE, &cur_key,
                   MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_VAL_TYPE, &cur_val, H5P_DEFAULT) < 0) {
            H5_FAILED();
            printf("    failed to set key-value pair %lld in map\n", (long long)i);
            goto error;
        }
    }

    /*
     * Re-open the map to ensure the keys make it.
     */
    if (H5Mclose(map_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close map");
        goto error;
    }

    if (H5Fclose(file_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close file");
        goto error;
    }

    if (H5daos_set_all_ind_metadata_ops(fapl_id, 1) < 0) {
        H5_FAILED();
        HDputs("    failed to set independent metadata writes");
        goto error;
    }

    if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDputs("    failed to open file");
        goto error;
    }

    if ((map_id = H5Mopen(file_id, MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_MAP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDputs("    failed to open map");
        goto error;
    }

    /*
     * Iterate over all keys, making sure that the count matches what is
     * expected and that the key values range from 0 to the number of keys inserted.
     */
    test_info.key_count   = 0;
    test_info.pass_number = 0;
    if (H5Miterate(map_id, NULL, MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_KEY_TYPE, iterate_func4,
                   &test_info, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDputs("    iteration over keys in map failed");
        goto error;
    }

    if (test_info.key_count != (size_t)(mpi_size * MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_N_KEYS)) {
        H5_FAILED();
        printf("    after first iteration, key count of %lld did not match expected value %lld\n",
               (long long)test_info.key_count,
               (long long)(mpi_size * MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_N_KEYS));
        goto error;
    }

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        HDputs("    MPI_Barrier failed");
        goto error;
    }

    /*
     * Have each rank delete a single key and then iterate over the remaining keys
     * until all keys are deleted this way.
     */
    for (i = 0; i < MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_N_KEYS; i++) {
        size_t expected_key_count;

        cur_key =
            (MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE)((size_t)(mpi_rank *
                                                              MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_N_KEYS) +
                                                     i);

        if (H5Mdelete(map_id, MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_KEY_TYPE, &cur_key, H5P_DEFAULT) <
            0) {
            H5_FAILED();
            HDputs("    failed to delete key in map\n");
            goto error;
        }

        /*
         * Re-open the map to ensure the keys are deleted.
         */
        if (H5Mclose(map_id) < 0) {
            H5_FAILED();
            HDputs("    failed to close map");
            goto error;
        }

        if (H5Fclose(file_id) < 0) {
            H5_FAILED();
            HDputs("    failed to close file");
            goto error;
        }

        if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDWR, fapl_id)) < 0) {
            H5_FAILED();
            HDputs("    failed to open file");
            goto error;
        }

        if ((map_id = H5Mopen(file_id, MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_MAP_NAME, H5P_DEFAULT)) <
            0) {
            H5_FAILED();
            HDputs("    failed to open map");
            goto error;
        }

        /*
         * Iterate over the keys and check the remaining amount,
         * as well as the values of the remaining keys. If deleted
         * properly, the values of the remaining keys should always
         * be greater than or equal to the pass number.
         */
        test_info.key_count   = 0;
        test_info.pass_number = i;
        if (H5Miterate(map_id, NULL, MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_KEY_TYPE, iterate_func4,
                       &test_info, H5P_DEFAULT) < 0) {
            H5_FAILED();
            HDputs("    iteration over keys in map failed");
            goto error;
        }

        expected_key_count = (size_t)((mpi_size * MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_N_KEYS) -
                                      (((int)i + 1) * mpi_size));
        if (test_info.key_count != expected_key_count) {
            H5_FAILED();
            printf("    after iteration %lld, key count of %lld did not match expected value %lld\n",
                   (long long)i, (long long)test_info.key_count, (long long)expected_key_count);
            goto error;
        }

        if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
            H5_FAILED();
            HDputs("    MPI_Barrier failed");
            goto error;
        }
    }

    if (H5Mclose(map_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close map");
        goto error;
    }

    if (H5Pclose(fapl_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close FAPL");
        goto error;
    }

    if (H5Fclose(file_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close file");
        goto error;
    }

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Mclose(map_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    }
    H5E_END_TRY;

    return 1;
}

/*
 * A test to create a map on all ranks, insert some keys on all ranks
 * and then continually have rank 0 update all of the map's keys. All
 * ranks will read back and verify each key-value pair on each iteration.
 */
#define MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_MAP_NAME   "update_rank_0_read_all_ranks_map"
#define MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_KEY_TYPE   H5T_NATIVE_INT
#define MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_VAL_TYPE   H5T_NATIVE_INT
#define MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_KEY_C_TYPE int
#define MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_VAL_C_TYPE int
#define MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_N_ITERS    10
#define MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_N_KEYS     10
static int
test_update_keys_rank_0_only_read_all_ranks()
{
    MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_KEY_C_TYPE cur_key;
    MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_VAL_C_TYPE cur_val;
    size_t                                           i;
    hid_t                                            file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t                                            map_id = H5I_INVALID_HID;

    TESTING_2("continually update keys on rank 0 - read and verify keys on all ranks");

    if ((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0) {
        H5_FAILED();
        HDputs("    failed to create FAPL");
        goto error;
    }

    if (H5Pset_all_coll_metadata_ops(fapl_id, 1) < 0) {
        H5_FAILED();
        HDputs("    failed to set collective metadata reads");
        goto error;
    }

    if (H5Pset_fapl_mpio(fapl_id, MPI_COMM_WORLD, MPI_INFO_NULL) < 0) {
        H5_FAILED();
        HDputs("    failed to set MPI on FAPL");
        goto error;
    }

    if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDputs("    failed to open file");
        goto error;
    }

    if ((map_id = H5Mcreate(file_id, MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_MAP_NAME,
                            MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_KEY_TYPE,
                            MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_VAL_TYPE, H5P_DEFAULT, H5P_DEFAULT,
                            H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDputs("    failed to create map");
        goto error;
    }

    /*
     * Insert some keys on all ranks.
     */
    for (i = 0; i < MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_N_KEYS; i++) {
        /* Keys range from 0 to ((mpi_size * MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_N_KEYS) - 1) */
        cur_key =
            (MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE)((size_t)(mpi_rank *
                                                              MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_N_KEYS) +
                                                     i);
        /* Values range from 0 to ((mpi_size * MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_N_KEYS) - 1) */
        cur_val =
            (MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE)((size_t)(mpi_rank *
                                                              MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_N_KEYS) +
                                                     i);

        if (H5Mput(map_id, MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_KEY_TYPE, &cur_key,
                   MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_VAL_TYPE, &cur_val, H5P_DEFAULT) < 0) {
            H5_FAILED();
            printf("    failed to set key-value pair %lld in map\n", (long long)i);
            goto error;
        }
    }

    /*
     * Re-open the map to ensure the keys make it.
     */
    if (H5Mclose(map_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close map");
        goto error;
    }

    if (H5Fclose(file_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close file");
        goto error;
    }

    if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDputs("    failed to open file");
        goto error;
    }

    if ((map_id = H5Mopen(file_id, MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_MAP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDputs("    failed to open map");
        goto error;
    }

    for (i = 0; i < (size_t)MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_N_ITERS; i++) {
        size_t j;

        /* Update all the keys to have a value of 1 more than before. */
        if (MAINPROCESS) {
            for (j = 0; j < (size_t)(mpi_size * MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_N_KEYS); j++) {
                cur_key = (MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE)j;

                if (H5Mget(map_id, MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_KEY_TYPE, &cur_key,
                           MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_VAL_TYPE, &cur_val, H5P_DEFAULT) < 0) {
                    H5_FAILED();
                    HDputs("    failed to retrieved key's value from map");
                    goto error;
                }

                cur_val++;

                if (H5Mput(map_id, MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_KEY_TYPE, &cur_key,
                           MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_VAL_TYPE, &cur_val, H5P_DEFAULT) < 0) {
                    H5_FAILED();
                    printf("    failed to set key-value pair %lld in map\n", (long long)j);
                    goto error;
                }
            }
        }

        if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
            H5_FAILED();
            HDputs("    MPI_Barrier failed");
            goto error;
        }

        /*
         * Re-open the map to ensure the key updates make it.
         */
        if (H5Mclose(map_id) < 0) {
            H5_FAILED();
            HDputs("    failed to close map");
            goto error;
        }

        if (H5Fclose(file_id) < 0) {
            H5_FAILED();
            HDputs("    failed to close file");
            goto error;
        }

        if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDWR, fapl_id)) < 0) {
            H5_FAILED();
            HDputs("    failed to open file");
            goto error;
        }

        if ((map_id = H5Mopen(file_id, MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_MAP_NAME, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDputs("    failed to open map");
            goto error;
        }

        /*
         * Check that all the keys were updated.
         */
        for (j = 0; j < (size_t)(mpi_size * MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_N_KEYS); j++) {
            cur_key = (MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE)j;

            if (H5Mget(map_id, MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_KEY_TYPE, &cur_key,
                       MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_VAL_TYPE, &cur_val, H5P_DEFAULT) < 0) {
                H5_FAILED();
                HDputs("    failed to retrieved key's value from map");
                goto error;
            }

            if (cur_val != (MAP_TEST_UPDATE_RANK_0_READ_ALL_RANKS_VAL_C_TYPE)((size_t)cur_key + i + 1)) {
                H5_FAILED();
                printf("    value %lld of key %lld did not match expected value %lld\n", (long long)cur_val,
                       (long long)j, (long long)((size_t)cur_key + i + 1));
                goto error;
            }
        }

        if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
            H5_FAILED();
            HDputs("    MPI_Barrier failed");
            goto error;
        }
    }

    if (H5Mclose(map_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close map");
        goto error;
    }

    if (H5Pclose(fapl_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close FAPL");
        goto error;
    }

    if (H5Fclose(file_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close file");
        goto error;
    }

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Mclose(map_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    }
    H5E_END_TRY;

    return 1;
}

/*
 * A test to create a map on all ranks, insert some keys on all ranks
 * and then continually have all ranks update their contributed keys. All
 * ranks will read back and verify each key-value pair on each iteration.
 */
#define MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_MAP_NAME   "update_all_ranks_read_all_ranks_map"
#define MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_KEY_TYPE   H5T_NATIVE_INT
#define MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_VAL_TYPE   H5T_NATIVE_INT
#define MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_KEY_C_TYPE int
#define MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_VAL_C_TYPE int
#define MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_N_ITERS    10
#define MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_N_KEYS     10
static int
test_update_keys_all_ranks_read_all_ranks()
{
    MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_KEY_C_TYPE cur_key;
    MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_VAL_C_TYPE cur_val;
    size_t                                              i;
    hid_t                                               file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t                                               map_id = H5I_INVALID_HID;

    TESTING_2("continually update keys on all ranks - read and verify keys on all ranks");

    if ((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0) {
        H5_FAILED();
        HDputs("    failed to create FAPL");
        goto error;
    }

    if (H5Pset_all_coll_metadata_ops(fapl_id, 1) < 0) {
        H5_FAILED();
        HDputs("    failed to set collective metadata reads");
        goto error;
    }

    if (H5Pset_fapl_mpio(fapl_id, MPI_COMM_WORLD, MPI_INFO_NULL) < 0) {
        H5_FAILED();
        HDputs("    failed to set MPI on FAPL");
        goto error;
    }

    if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDputs("    failed to open file");
        goto error;
    }

    if ((map_id = H5Mcreate(file_id, MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_MAP_NAME,
                            MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_KEY_TYPE,
                            MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_VAL_TYPE, H5P_DEFAULT, H5P_DEFAULT,
                            H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDputs("    failed to create map");
        goto error;
    }

    /*
     * Insert some keys on all ranks.
     */
    for (i = 0; i < MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_N_KEYS; i++) {
        /* Keys range from 0 to ((mpi_size * MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_N_KEYS) - 1) */
        cur_key =
            (MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE)((size_t)(mpi_rank *
                                                              MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_N_KEYS) +
                                                     i);
        /* Values range from 0 to ((mpi_size * MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_N_KEYS) - 1) */
        cur_val =
            (MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE)((size_t)(mpi_rank *
                                                              MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_N_KEYS) +
                                                     i);

        if (H5Mput(map_id, MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_KEY_TYPE, &cur_key,
                   MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_VAL_TYPE, &cur_val, H5P_DEFAULT) < 0) {
            H5_FAILED();
            printf("    failed to set key-value pair %lld in map\n", (long long)i);
            goto error;
        }
    }

    /*
     * Re-open the map to ensure the keys make it.
     */
    if (H5Mclose(map_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close map");
        goto error;
    }

    if (H5Fclose(file_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close file");
        goto error;
    }

    if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDWR, fapl_id)) < 0) {
        H5_FAILED();
        HDputs("    failed to open file");
        goto error;
    }

    if ((map_id = H5Mopen(file_id, MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_MAP_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        HDputs("    failed to open map");
        goto error;
    }

    for (i = 0; i < (size_t)MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_N_ITERS; i++) {
        size_t j;

        /* Update all the keys to have a value of 1 more than before. */
        for (j = 0; j < (size_t)MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_N_KEYS; j++) {
            cur_key =
                (MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE)((size_t)(mpi_rank *
                                                                  MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_N_KEYS) +
                                                         j);

            if (H5Mget(map_id, MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_KEY_TYPE, &cur_key,
                       MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_VAL_TYPE, &cur_val, H5P_DEFAULT) < 0) {
                H5_FAILED();
                HDputs("    failed to retrieved key's value from map");
                goto error;
            }

            cur_val++;

            if (H5Mput(map_id, MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_KEY_TYPE, &cur_key,
                       MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_VAL_TYPE, &cur_val, H5P_DEFAULT) < 0) {
                H5_FAILED();
                printf("    failed to set key-value pair %lld in map\n", (long long)j);
                goto error;
            }
        }

        if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
            H5_FAILED();
            HDputs("    MPI_Barrier failed");
            goto error;
        }

        /*
         * Re-open the map to ensure the key updates make it.
         */
        if (H5Mclose(map_id) < 0) {
            H5_FAILED();
            HDputs("    failed to close map");
            goto error;
        }

        if (H5Fclose(file_id) < 0) {
            H5_FAILED();
            HDputs("    failed to close file");
            goto error;
        }

        if ((file_id = H5Fopen(PARALLEL_FILENAME, H5F_ACC_RDWR, fapl_id)) < 0) {
            H5_FAILED();
            HDputs("    failed to open file");
            goto error;
        }

        if ((map_id = H5Mopen(file_id, MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_MAP_NAME, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDputs("    failed to open map");
            goto error;
        }

        /*
         * Check that all the keys were updated.
         */
        for (j = 0; j < (size_t)(mpi_size * MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_N_KEYS); j++) {
            cur_key = (MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE)j;

            if (H5Mget(map_id, MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_KEY_TYPE, &cur_key,
                       MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_VAL_TYPE, &cur_val, H5P_DEFAULT) < 0) {
                H5_FAILED();
                HDputs("    failed to retrieved key's value from map");
                goto error;
            }

            if (cur_val != (MAP_TEST_UPDATE_ALL_RANKS_READ_ALL_RANKS_VAL_C_TYPE)((size_t)cur_key + i + 1)) {
                H5_FAILED();
                printf("    value %lld of key %lld did not match expected value %lld\n", (long long)cur_val,
                       (long long)j, (long long)((size_t)cur_key + i + 1));
                goto error;
            }
        }

        if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
            H5_FAILED();
            HDputs("    MPI_Barrier failed");
            goto error;
        }
    }

    if (H5Mclose(map_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close map");
        goto error;
    }

    if (H5Pclose(fapl_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close FAPL");
        goto error;
    }

    if (H5Fclose(file_id) < 0) {
        H5_FAILED();
        HDputs("    failed to close file");
        goto error;
    }

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Mclose(map_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    }
    H5E_END_TRY;

    return 1;
}

/*
 * A key iteration function for the test_insert_keys_one_rank_iterate_all_ranks
 * test which counts the number of keys and makes sure that each key
 * is a multiple of 10.
 */
static herr_t
iterate_func1(hid_t map_id, const void *key, void *op_data)
{
    MAP_TEST_INSERT_RANK_0_ITERATE_ALL_RANKS_VAL_C_TYPE retrieved_val;
    size_t                                             *counter = (size_t *)op_data;
    herr_t                                              ret_val = H5_ITER_CONT;

    if (H5Mget(map_id, MAP_TEST_INSERT_RANK_0_ITERATE_ALL_RANKS_KEY_TYPE, key,
               MAP_TEST_INSERT_RANK_0_ITERATE_ALL_RANKS_VAL_TYPE, &retrieved_val, H5P_DEFAULT)) {
        H5_FAILED();
        HDputs("    failed to retrieved key's value from map");
        ret_val = H5_ITER_ERROR;
        goto done;
    }

    if (retrieved_val % 10 != 0) {
        H5_FAILED();
        printf("    key value was expected to be a multiple of 10, but was %lld\n", (long long)retrieved_val);
        ret_val = H5_ITER_ERROR;
        goto done;
    }

done:
    if (counter)
        (*counter)++;

    return ret_val;
}

/*
 * A key iteration function for the test_insert_keys_all_ranks_iterate_all_ranks
 * test which counts the number of keys and makes sure that each key
 * is a multiple of 5.
 */
static herr_t
iterate_func2(hid_t map_id, const void *key, void *op_data)
{
    MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_VAL_C_TYPE retrieved_val;
    size_t                                                *counter = (size_t *)op_data;
    herr_t                                                 ret_val = H5_ITER_CONT;

    if (H5Mget(map_id, MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_KEY_TYPE, key,
               MAP_TEST_INSERT_ALL_RANKS_ITERATE_ALL_RANKS_VAL_TYPE, &retrieved_val, H5P_DEFAULT)) {
        H5_FAILED();
        HDputs("    failed to retrieved key's value from map");
        ret_val = H5_ITER_ERROR;
        goto done;
    }

    if (retrieved_val % 5 != 0) {
        H5_FAILED();
        printf("    key value was expected to be a multiple of 5, but was %lld\n", (long long)retrieved_val);
        ret_val = H5_ITER_ERROR;
        goto done;
    }

done:
    if (counter)
        (*counter)++;

    return ret_val;
}

/*
 * A key iteration function for the test_delete_keys_one_rank_iterate_all_ranks
 * test which counts the number of keys and makes sure that each key
 * is a multiple of 5. On the second iteration, this function makes sure that
 * none of the keys have a value of 0, since those keys should have been deleted.
 */
static herr_t
iterate_func3(hid_t map_id, const void *key, void *op_data)
{
    MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_VAL_C_TYPE retrieved_val;
    delete_rank_0_test_info                            *test_info  = (delete_rank_0_test_info *)op_data;
    hbool_t                                             key_exists = 0;
    herr_t                                              ret_val    = H5_ITER_CONT;

    if (H5Mexists(map_id, MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_KEY_TYPE, key, &key_exists, H5P_DEFAULT) <
        0) {
        H5_FAILED();
        HDputs("    failed to determine if key exists in map");
        ret_val = H5_ITER_ERROR;
        goto done;
    }

    if (key_exists) {
        if (H5Mget(map_id, MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_KEY_TYPE, key,
                   MAP_TEST_DELETE_RANK_0_ITERATE_ALL_RANKS_VAL_TYPE, &retrieved_val, H5P_DEFAULT)) {
            H5_FAILED();
            HDputs("    failed to retrieved key's value from map");
            ret_val = H5_ITER_ERROR;
            goto done;
        }

        if (retrieved_val % 5 != 0) {
            H5_FAILED();
            printf("    key value was expected to be a multiple of 5, but was %lld\n",
                   (long long)retrieved_val);
            ret_val = H5_ITER_ERROR;
            goto done;
        }

        if (test_info->second_pass) {
            /*
             * Make sure none of they keys have a value of 0.
             */
            if (retrieved_val == 0) {
                H5_FAILED();
                HDputs("    a deleted key still appeared during iteration");
                ret_val = H5_ITER_ERROR;
                goto done;
            }
        }
    }

done:
    if (test_info && key_exists)
        test_info->key_count++;

    return ret_val;
}

/*
 * A key iteration function for the test_delete_keys_all_ranks_iterate_all_ranks
 * test which counts the number of keys and on each iteration makes sure that each
 * key value is greater than or equal to the given pass number.
 */
static herr_t
iterate_func4(hid_t map_id, const void *key, void *op_data)
{
    MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_VAL_C_TYPE retrieved_val;
    delete_all_ranks_test_info                            *test_info  = (delete_all_ranks_test_info *)op_data;
    hbool_t                                                key_exists = 0;
    herr_t                                                 ret_val    = H5_ITER_CONT;

    if (H5Mexists(map_id, MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_KEY_TYPE, key, &key_exists,
                  H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDputs("    failed to determine if key exists in map");
        ret_val = H5_ITER_ERROR;
        goto done;
    }

    if (key_exists) {
        if (H5Mget(map_id, MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_KEY_TYPE, key,
                   MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_VAL_TYPE, &retrieved_val, H5P_DEFAULT)) {
            H5_FAILED();
            HDputs("    failed to retrieved key's value from map");
            ret_val = H5_ITER_ERROR;
            goto done;
        }

        if (retrieved_val < (MAP_TEST_DELETE_ALL_RANKS_ITERATE_ALL_RANKS_VAL_C_TYPE)test_info->pass_number) {
            H5_FAILED();
            printf("    key value was expected to be greater than or equal to the current iteration number "
                   "%lld, but was %lld\n",
                   (long long)test_info->pass_number, (long long)retrieved_val);
            ret_val = H5_ITER_ERROR;
            goto done;
        }
    }

done:
    if (test_info && key_exists)
        test_info->key_count++;

    return ret_val;
}

int
main(int argc, char **argv)
{
    hid_t file_id = H5I_INVALID_HID;
    int   nerrors = 0;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);

    /*
     * Make sure that HDF5 is initialized on all MPI ranks before proceeding.
     */
    H5open();

    srand((unsigned)time(NULL));

    if (MAINPROCESS) {
        if ((file_id = H5Fcreate(PARALLEL_FILENAME, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
            HDputs("    failed to create file");
            AT();
            nerrors++;
            goto error;
        }

        if (H5Fclose(file_id) < 0) {
            HDputs("    failed to close file");
            AT();
            nerrors++;
            goto error;
        }
    }

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        HDputs("    MPI_Barrier failed after file creation");
        nerrors++;
        goto error;
    }

    nerrors += test_create_map_rank_0();
    nerrors += test_create_map_all_ranks();
    nerrors += test_insert_keys_one_rank();
    nerrors += test_insert_keys_all_ranks();
    nerrors += test_insert_keys_one_rank_iterate_all_ranks();
    nerrors += test_insert_keys_all_ranks_iterate_all_ranks();
    nerrors += test_delete_keys_one_rank_iterate_all_ranks();
    nerrors += test_delete_keys_all_ranks_iterate_all_ranks();
    nerrors += test_update_keys_rank_0_only_read_all_ranks();
    nerrors += test_update_keys_all_ranks_read_all_ranks();

    if (nerrors)
        goto error;

    if (MAINPROCESS)
        puts("All DAOS Parallel Map tests passed");

    MPI_Finalize();

    return 0;

error:
    if (MAINPROCESS)
        printf("*** %d TEST%s FAILED ***\n", nerrors, (!nerrors || nerrors > 1) ? "S" : "");

    MPI_Finalize();

    return 1;
}
