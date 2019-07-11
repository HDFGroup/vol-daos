/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of the HDF5 DAOS VOL connector. The full copyright      *
 * notice, including terms governing use, modification, and redistribution,  *
 * is contained in the COPYING file, which can be found at the root of the   *
 * source code distribution tree.                                            *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Purpose: Tests H5M (Map API) interface in parallel.
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <hdf5.h>

#include "daos_vol_public.h"
#include "h5daos_test.h"

#define PARALLEL_FILENAME "h5_daos_test_map_parallel.h5"

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

    TESTING_2("creation of map object on rank 0 only - reopen on all ranks")

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

        if ((map_id = H5Mcreate(file_id, MAP_TEST_CREATE_RANK_0_ONLY_MAP_NAME, MAP_TEST_CREATE_RANK_0_ONLY_KEY_TYPE,
                MAP_TEST_CREATE_RANK_0_ONLY_VAL_TYPE, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
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
    H5E_BEGIN_TRY {
        H5Mclose(map_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

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

    TESTING_2("creation of map object on all ranks - reopen on all ranks")

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
    H5E_BEGIN_TRY {
        H5Mclose(map_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

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
    hid_t file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t map_id = H5I_INVALID_HID;

    TESTING_2("inserting keys on rank 0 then retrieving values on all ranks")

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

        if ((map_id = H5Mcreate(file_id, MAP_TEST_INSERT_RANK_0_ONLY_MAP_NAME, MAP_TEST_INSERT_RANK_0_ONLY_KEY_TYPE,
                MAP_TEST_INSERT_RANK_0_ONLY_VAL_TYPE, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            HDputs("    failed to create map");
            goto error;
        }

        /*
         * Insert a few keys
         */
        if (H5Mset(map_id, MAP_TEST_INSERT_RANK_0_ONLY_KEY_TYPE, &key1,
                MAP_TEST_INSERT_RANK_0_ONLY_VAL_TYPE, &val1, H5P_DEFAULT)) {
            H5_FAILED();
            HDputs("    failed to set key-value pair in map");
            goto error;
        }

        if (H5Mset(map_id, MAP_TEST_INSERT_RANK_0_ONLY_KEY_TYPE, &key2,
                MAP_TEST_INSERT_RANK_0_ONLY_VAL_TYPE, &val2, H5P_DEFAULT)) {
            H5_FAILED();
            HDputs("    failed to set key-value pair in map");
            goto error;
        }

        if (H5Mset(map_id, MAP_TEST_INSERT_RANK_0_ONLY_KEY_TYPE, &key3,
                MAP_TEST_INSERT_RANK_0_ONLY_VAL_TYPE, &val3, H5P_DEFAULT)) {
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
    if (H5Mget(map_id, MAP_TEST_INSERT_RANK_0_ONLY_KEY_TYPE, &key1,
            MAP_TEST_INSERT_RANK_0_ONLY_VAL_TYPE, &val1, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDputs("    failed to retrieve value by key from map");
        goto error;
    }

    if (val1 != 10) {
        H5_FAILED();
        HDputs("    value for first key did not match expected value");
        goto error;
    }

    if (H5Mget(map_id, MAP_TEST_INSERT_RANK_0_ONLY_KEY_TYPE, &key2,
            MAP_TEST_INSERT_RANK_0_ONLY_VAL_TYPE, &val2, H5P_DEFAULT) < 0) {
        H5_FAILED();
        HDputs("    failed to retrieve value by key from map");
        goto error;
    }

    if (val2 != 20) {
        H5_FAILED();
        HDputs("    value for second key did not match expected value");
        goto error;
    }

    if (H5Mget(map_id, MAP_TEST_INSERT_RANK_0_ONLY_KEY_TYPE, &key3,
            MAP_TEST_INSERT_RANK_0_ONLY_VAL_TYPE, &val3, H5P_DEFAULT) < 0) {
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
    H5E_BEGIN_TRY {
        H5Mclose(map_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

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
    MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE *keys = NULL;
    MAP_TEST_INSERT_RANK_0_ONLY_KEY_C_TYPE *retrieved_keys = NULL;
    MAP_TEST_INSERT_RANK_0_ONLY_VAL_C_TYPE *vals = NULL;
    MAP_TEST_INSERT_RANK_0_ONLY_VAL_C_TYPE *retrieved_vals = NULL;
#ifndef NO_GET_MAP_COUNT
    hsize_t key_count;
#endif
    size_t  i;
    hid_t   file_id = H5I_INVALID_HID, fapl_id = H5I_INVALID_HID;
    hid_t   map_id = H5I_INVALID_HID;

    TESTING_2("inserting different keys on all ranks then retrieving values on all ranks")

    if (NULL == (keys = (MAP_TEST_INSERT_ALL_RANKS_KEY_C_TYPE *) malloc(MAP_TEST_INSERT_ALL_RANKS_N_KEYS_PER_RANK * sizeof(MAP_TEST_INSERT_ALL_RANKS_KEY_C_TYPE)))) {
        H5_FAILED();
        HDputs("    failed to allocate buffer for map keys");
        goto error;
    }

    if (NULL == (vals = (MAP_TEST_INSERT_ALL_RANKS_VAL_C_TYPE *) malloc(MAP_TEST_INSERT_ALL_RANKS_N_KEYS_PER_RANK * sizeof(MAP_TEST_INSERT_ALL_RANKS_VAL_C_TYPE)))) {
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
        keys[i] = (mpi_rank * MAP_TEST_INSERT_ALL_RANKS_N_KEYS_PER_RANK) + i;
        /* Values simply determined by the current rank number */
        vals[i] = mpi_rank;

        if (H5Mset(map_id, MAP_TEST_INSERT_ALL_RANKS_KEY_TYPE, &(keys[i]),
                MAP_TEST_INSERT_ALL_RANKS_VAL_TYPE, &(vals[i]), H5P_DEFAULT) < 0) {
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

    if (key_count != (hsize_t) (mpi_size * MAP_TEST_INSERT_ALL_RANKS_N_KEYS_PER_RANK)) {
        H5_FAILED();
        printf("    map's key count of %lld did not match expected value %lld", (long long) key_count, (long long) (mpi_size * MAP_TEST_INSERT_ALL_RANKS_N_KEYS_PER_RANK));
        goto error;
    }
#endif

    if (NULL == (retrieved_keys = (MAP_TEST_INSERT_ALL_RANKS_KEY_C_TYPE *) malloc(MAP_TEST_INSERT_ALL_RANKS_N_KEYS_PER_RANK * mpi_size * sizeof(MAP_TEST_INSERT_ALL_RANKS_KEY_C_TYPE)))) {
        H5_FAILED();
        HDputs("    failed to allocate buffer for retrieved map keys");
        goto error;
    }

    if (NULL == (retrieved_vals = (MAP_TEST_INSERT_ALL_RANKS_VAL_C_TYPE *) malloc(MAP_TEST_INSERT_ALL_RANKS_N_KEYS_PER_RANK * mpi_size * sizeof(MAP_TEST_INSERT_ALL_RANKS_VAL_C_TYPE)))) {
        H5_FAILED();
        HDputs("    failed to allocate buffer for retrieved map values");
        goto error;
    }

    for (i = 0; i < (size_t) mpi_size * MAP_TEST_INSERT_ALL_RANKS_N_KEYS_PER_RANK; i++) {
        retrieved_keys[i] = i;
        retrieved_vals[i] = -1;

        if (H5Mget(map_id, MAP_TEST_INSERT_ALL_RANKS_KEY_TYPE, &(retrieved_keys[i]),
                MAP_TEST_INSERT_ALL_RANKS_VAL_TYPE, &(retrieved_vals[i]), H5P_DEFAULT) < 0) {
            H5_FAILED();
            HDputs("    failed to retrieve value by key from map");
            goto error;
        }

        if (retrieved_vals[i] != (MAP_TEST_INSERT_ALL_RANKS_KEY_C_TYPE) (i / MAP_TEST_INSERT_ALL_RANKS_N_KEYS_PER_RANK)) {
            H5_FAILED();
            printf("    value for key %lld did not match expected value", (long long) i);
            goto error;
        }
    }

    free(keys); keys = NULL;
    free(retrieved_keys); retrieved_keys = NULL;
    free(vals); vals = NULL;
    free(retrieved_vals); retrieved_vals = NULL;

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
    H5E_BEGIN_TRY {
        if (keys) free(keys);
        if (retrieved_keys) free(retrieved_keys);
        if (vals) free(vals);
        if (retrieved_vals) free(retrieved_vals);
        H5Mclose(map_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
}

static int
test_insert_same_key_all_ranks()
{
    return 1;
}

static int
test_insert_keys_one_rank_iterate_all_ranks()
{
    return 1;
}

static int
test_insert_keys_all_ranks_iterate_all_ranks()
{
    return 1;
}

static int
test_delete_keys_one_rank_iterate_all_ranks()
{
    return 1;
}

static int
test_delete_keys_all_ranks_iterate_all_ranks()
{
    return 1;
}

static int
test_delete_same_key_all_ranks()
{
    return 1;
}

static int
test_update_keys_rank_0_only_read_all_ranks()
{
    return 1;
}

static int
test_update_keys_all_ranks_read_all_ranks()
{
    return 1;
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

    srand((unsigned) time(NULL));

    if (MAINPROCESS) {
        if ((file_id = H5Fcreate(PARALLEL_FILENAME, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
            HDputs("    failed to create file"); AT();
            nerrors++;
            goto error;
        }

        if (H5Fclose(file_id) < 0) {
            HDputs("    failed to close file"); AT();
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
    nerrors += test_insert_same_key_all_ranks();
    nerrors += test_insert_keys_one_rank_iterate_all_ranks();
    nerrors += test_insert_keys_all_ranks_iterate_all_ranks();
    nerrors += test_delete_keys_one_rank_iterate_all_ranks();
    nerrors += test_delete_keys_all_ranks_iterate_all_ranks();
    nerrors += test_delete_same_key_all_ranks();
    nerrors += test_update_keys_rank_0_only_read_all_ranks();
    nerrors += test_update_keys_all_ranks_read_all_ranks();

    if (nerrors) goto error;

    if (MAINPROCESS) puts("All DAOS Parallel Map tests passed");

    MPI_Finalize();

    return 0;

error:
    if (MAINPROCESS) printf("*** %d TEST%s FAILED ***\n", nerrors, (!nerrors || nerrors > 1) ? "S" : "");

    MPI_Finalize();

    return 1;
}
