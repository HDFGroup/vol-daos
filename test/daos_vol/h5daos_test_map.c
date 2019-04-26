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
 * Purpose: Tests H5M (Map API) interface
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <hdf5.h>

#include "daos_vol_public.h"
#include "h5daos_test.h"

/*
 * Definitions
 */
#define FILENAME                "h5daos_test_map.h5"
#define MAP_INT_INT_NAME        "map_int_int"
#define INT_INT_NKEYS           4   /* Don't set this too high because this test uses linear search */

/*
 * Global variables
 */
uuid_t pool_uuid;
int    mpi_rank;

/* Keys and values for int-int map */
int int_int_keys[INT_INT_NKEYS];
int int_int_vals[INT_INT_NKEYS];

/*
 * Tests creating and closing a map object
 */
static int
test_create_map(void)
{
    hid_t file_id = -1, fapl_id = -1;
    hid_t map_id = -1;

    TESTING("creation of map object")

    if((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0)
        TEST_ERROR
    if(H5Pset_all_coll_metadata_ops(fapl_id, true) < 0)
        TEST_ERROR

    if((file_id = H5Fcreate(FILENAME, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0)
        TEST_ERROR

    if((map_id = H5Mcreate(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED(); AT();
        printf("    couldn't create map\n");
        goto error;
    } /* end if */

    if(H5Mclose(map_id) < 0)
        TEST_ERROR
    if(H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if(H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Mclose(map_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
} /* end test_create_map() */


/*
 * Tests opening a map object
 */
static int
test_open_map(void)
{
    hid_t file_id = -1, fapl_id = -1;
    hid_t map_id = -1;

    TESTING("open of map object")

    if((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0)
        TEST_ERROR
    if(H5Pset_all_coll_metadata_ops(fapl_id, true) < 0)
        TEST_ERROR

    if((file_id = H5Fopen(FILENAME, H5F_ACC_RDONLY, fapl_id)) < 0)
        TEST_ERROR

    if((map_id = H5Mopen(file_id, MAP_INT_INT_NAME, H5P_DEFAULT)) < 0) {
        H5_FAILED(); AT();
        printf("    couldn't open map\n");
        goto error;
    } /* end if */

    if(H5Mclose(map_id) < 0)
        TEST_ERROR
    if(H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if(H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Mclose(map_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
} /* end test_create_map() */


/*
 * Tests setting keys in a map object
 */
static int
test_map_set_int_int(void)
{
    hid_t file_id = -1, fapl_id = -1;
    hid_t map_id = -1;
    int i;

    TESTING("map set with integer keys and values")

    if((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0)
        TEST_ERROR
    if(H5Pset_all_coll_metadata_ops(fapl_id, true) < 0)
        TEST_ERROR

    if((file_id = H5Fopen(FILENAME, H5F_ACC_RDWR, fapl_id)) < 0)
        TEST_ERROR

    if((map_id = H5Mopen(file_id, MAP_INT_INT_NAME, H5P_DEFAULT)) < 0)
        TEST_ERROR

    /* Set the values */
    for(i = 0; i < INT_INT_NKEYS; i++)
        if(H5Mset(map_id, H5T_NATIVE_INT, &int_int_keys[i], H5T_NATIVE_INT, &int_int_vals[i], H5P_DEFAULT) < 0) {
            H5_FAILED(); AT();
            printf("failed to set key-value pair\n");
            goto error;
        } /* end if */

    if(H5Mclose(map_id) < 0)
        TEST_ERROR
    if(H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if(H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Mclose(map_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
} /* end test_map_set_int_int() */


/*
 * Tests getting keys from a map object
 */
static int
test_map_get_int_int(void)
{
    hid_t file_id = -1, fapl_id = -1;
    hid_t map_id = -1;
    int vals_out[INT_INT_NKEYS];
    int i;

    TESTING("map get with integer keys and values")

    if((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0)
        TEST_ERROR
    if(H5Pset_all_coll_metadata_ops(fapl_id, true) < 0)
        TEST_ERROR

    if((file_id = H5Fopen(FILENAME, H5F_ACC_RDONLY, fapl_id)) < 0)
        TEST_ERROR

    if((map_id = H5Mopen(file_id, MAP_INT_INT_NAME, H5P_DEFAULT)) < 0)
        TEST_ERROR

    /* Get the values and check that they are correct */
    for(i = 0; i < INT_INT_NKEYS; i++) {
        if(H5Mget(map_id, H5T_NATIVE_INT, &int_int_keys[i], H5T_NATIVE_INT, &vals_out[i], H5P_DEFAULT) < 0) {
            H5_FAILED(); AT();
            printf("failed to get key-value pair\n");
            goto error;
        } /* end if */

        if(vals_out[i] != int_int_vals[i]) {
            H5_FAILED(); AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */
    } /* end for */

    if(H5Mclose(map_id) < 0)
        TEST_ERROR
    if(H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if(H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Mclose(map_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
} /* end test_map_get_int_int() */


/*
 * Tests checking if keys exist in a map object
 */
static int
test_map_exists_int_int(void)
{
    hid_t file_id = -1, fapl_id = -1;
    hid_t map_id = -1;
    hbool_t exists;
    int nonexist_key;
    int i;

    TESTING("map exists with integer keys and values")

    if((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0)
        TEST_ERROR
    if(H5Pset_all_coll_metadata_ops(fapl_id, true) < 0)
        TEST_ERROR

    if((file_id = H5Fopen(FILENAME, H5F_ACC_RDONLY, fapl_id)) < 0)
        TEST_ERROR

    if((map_id = H5Mopen(file_id, MAP_INT_INT_NAME, H5P_DEFAULT)) < 0)
        TEST_ERROR

    /* Check if the existing keys exist (should all be TRUE) */
    for(i = 0; i < INT_INT_NKEYS; i++) {
        if(H5Mexists(map_id, H5T_NATIVE_INT, &int_int_keys[i], &exists, H5P_DEFAULT) < 0) {
            H5_FAILED(); AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if(!exists) {
            H5_FAILED(); AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */
    } /* end for */

    /* Look for key that does not exist */
    nonexist_key = -1;
    do {
        nonexist_key++;
        exists = 0;
        for(i = 0; i < INT_INT_NKEYS; i++)
            if(int_int_keys[i] == nonexist_key) {
                exists = 1;
                break;
            } /* end if */
    } while(exists);

    /* Check if the nonexisting key exists (should be FALSE) */
    if(H5Mexists(map_id, H5T_NATIVE_INT, &nonexist_key, &exists, H5P_DEFAULT) < 0) {
        H5_FAILED(); AT();
        printf("failed to check if key exists\n");
        goto error;
    } /* end if */

    if(exists) {
        H5_FAILED(); AT();
        printf("incorrect value returned\n");
        goto error;
    } /* end if */

    if(H5Mclose(map_id) < 0)
        TEST_ERROR
    if(H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if(H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Mclose(map_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
} /* end test_map_exists_int_int() */


/*
 * Tests iterating over all keys in a map object
 */
typedef struct {
    int keys_visited[INT_INT_NKEYS];
    int ncalls;
    int stop_at;
} iterate_int_int_ud_t;

static herr_t
map_iterate_int_int_cb(hid_t map_id, const void *_key, void *_iterate_ud)
{
    const int *key = (const int *)_key;
    iterate_int_int_ud_t *iterate_ud = (iterate_int_int_ud_t *)_iterate_ud;
    int i;

    /* Check parameters */
    if(!key) {
        H5_FAILED(); AT();
        printf("key is NULL\n");
        goto error;
    } /* end if */
    if(!iterate_ud) {
        H5_FAILED(); AT();
        printf("op_data is NULL\n");
        goto error;
    } /* end if */

    /* Mark key visited */
    for(i = 0; i < INT_INT_NKEYS; i++)
        if(int_int_keys[i] == *key) {
            iterate_ud->keys_visited[i]++;
            break;
        } /* end if */
    if(i == INT_INT_NKEYS) {
        H5_FAILED(); AT();
        printf("key not found\n");
        goto error;
    } /* end if */

    /* Check for short circuit */
    if(++iterate_ud->ncalls == iterate_ud->stop_at)
        return 1;

    return 0;

error:
    return -1;
} /* end map_iterate_int_int_cb */

static int
test_map_iterate_int_int(void)
{
    hid_t file_id = -1, fapl_id = -1;
    hid_t map_id = -1;
    iterate_int_int_ud_t iterate_ud;
    hsize_t idx;
    int nkeys;
    int ret;
    int i;

    TESTING("iterating over keys in map")

    if((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0)
        TEST_ERROR
    if(H5Pset_all_coll_metadata_ops(fapl_id, true) < 0)
        TEST_ERROR

    if((file_id = H5Fopen(FILENAME, H5F_ACC_RDONLY, fapl_id)) < 0)
        TEST_ERROR

    if((map_id = H5Mopen(file_id, MAP_INT_INT_NAME, H5P_DEFAULT)) < 0)
        TEST_ERROR

    /* Reset iterate_cb */
    memset(&iterate_ud, 0, sizeof(iterate_ud));

    /* Iterate over all keys */
    iterate_ud.stop_at = -1;
    idx = 0;
    if((ret = H5Miterate(map_id, &idx, H5T_NATIVE_INT, map_iterate_int_int_cb, &iterate_ud, H5P_DEFAULT)) < 0) {
        H5_FAILED(); AT();
        printf("failed to iterate over map\n");
        goto error;
    } /* end if */
    if(ret != 0) {
        H5_FAILED(); AT();
        printf("incorrect return code from H5Miterate\n");
        goto error;
    } /* end if */
    if(idx != (hsize_t)INT_INT_NKEYS){
        H5_FAILED(); AT();
        printf("incorrect value of idx after H5Miterate\n");
        goto error;
    } /* end if */

    /* Check that all keys were visited exactly once */
    for(i = 0; i < INT_INT_NKEYS; i++)
        if(iterate_ud.keys_visited[i] != 1) {
            H5_FAILED(); AT();
            printf("key visited an incorrect number of times\n");
            goto error;
        } /* end if */

    /* Reset iterate_cb */
    memset(&iterate_ud, 0, sizeof(iterate_ud));

    /* Iterate but stop after the second key */
    iterate_ud.stop_at = 2;
    idx = 0;
    if((ret = H5Miterate(map_id, &idx, H5T_NATIVE_INT, map_iterate_int_int_cb, &iterate_ud, H5P_DEFAULT)) < 0) {
        H5_FAILED(); AT();
        printf("failed to iterate over map\n");
        goto error;
    } /* end if */
    if(ret != 1) {
        H5_FAILED(); AT();
        printf("incorrect return code from H5Miterate\n");
        goto error;
    } /* end if */
    if(idx != (hsize_t)2){
        H5_FAILED(); AT();
        printf("incorrect value of idx after H5Miterate\n");
        goto error;
    } /* end if */

    /* Check that all keys were visited zero or one times, and that exactly two
     * keys were visited in total. */
    nkeys = 0;
    for(i = 0; i < INT_INT_NKEYS; i++) {
        if((iterate_ud.keys_visited[i] != 0)
                && (iterate_ud.keys_visited[i] != 1)) {
            H5_FAILED(); AT();
            printf("key visited an incorrect number of times\n");
            goto error;
        } /* end if */
        nkeys += iterate_ud.keys_visited[i];
    } /* end for */
    if(nkeys != 2) {
        H5_FAILED(); AT();
        printf("incorrect number of keys visited\n");
        goto error;
    } /* end if */

    if(H5Mclose(map_id) < 0)
        TEST_ERROR
    if(H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if(H5Fclose(file_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Mclose(map_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
    } H5E_END_TRY;

    return 1;
} /* end test_map_iterate_int_int() */


/*
 * main function
 */
int
main( int argc, char** argv )
{
    int     i;
    int     nerrors = 0;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

    srand((unsigned) time(NULL));

    /* Generate random keys and values */
    for(i = 0; i < INT_INT_NKEYS; i++) {
        int_int_keys[i] = rand();
        int_int_vals[i] = rand();
    } /* end for */

    nerrors += test_create_map();
    nerrors += test_open_map();
    nerrors += test_map_set_int_int();
    nerrors += test_map_get_int_int();
    nerrors += test_map_exists_int_int();
    nerrors += test_map_iterate_int_int();

    if (nerrors) goto error;

    if (MAINPROCESS) puts("All DAOS Map tests passed");

    MPI_Finalize();

    return 0;

error:
    if (MAINPROCESS) printf("*** %d TEST%s FAILED ***\n", nerrors, (!nerrors || nerrors > 1) ? "S" : "");

    MPI_Finalize();

    return 1;
} /* end main() */

