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
#define NUMB_KEYS               4
#define LARGE_NUMB_KEYS         1024   /* Don't set this too high because this test uses linear search */
#define LARGE_NUMB_MAPS         128

#define MAP_INT_INT_NAME        "map_int_int"
#define MAP_ENUM_ENUM_NAME      "map_enum_enum"
#define MAP_VL_VL_NAME          "map_vl_vl"
#define MAP_COMP_COMP_NAME      "map_comp_comp"
#define MAP_LARGE_NAME          "map_large"
#define MAP_NONEXISTENT_MAP     "map_nonexistent"

#define CPTR(VAR,CONST) ((VAR)=(CONST),&(VAR))

/*
 * Global variables
 */
uuid_t pool_uuid;
int    mpi_rank;

/* Keys and values for int-int map */
int int_int_keys[NUMB_KEYS];
int int_int_vals[NUMB_KEYS];
int int_vals_out[NUMB_KEYS];

int large_int_int_keys[LARGE_NUMB_KEYS];
int large_int_int_vals[LARGE_NUMB_KEYS];
int large_int_vals_out[LARGE_NUMB_KEYS];

int random_base;

/* Keys and values for enum-enum map */
typedef enum {
    ONE,
    TWO,
    THREE,
    FOUR,
    FIVE
} enum_key_t;

typedef enum {
    RED,
    GREEN,
    BLUE,
    WHITE,
    BLACK
} enum_value_t;

enum_key_t   enum_enum_keys[NUMB_KEYS];
enum_value_t enum_enum_vals[NUMB_KEYS];
enum_value_t enum_vals_out[NUMB_KEYS];

/* Keys and values for vl-vl map */
hvl_t vl_vl_keys[NUMB_KEYS];
hvl_t vl_vl_vals[NUMB_KEYS];
hvl_t vl_vals_out[NUMB_KEYS];

/* Keys and values for compound-compound map */
typedef struct compound_t {
    int    a;
    float  b;
} compound_t;

compound_t comp_comp_keys[NUMB_KEYS];
compound_t comp_comp_vals[NUMB_KEYS];
compound_t comp_vals_out[NUMB_KEYS];

/*
 * Tests creating and closing a map object
 */
static int
test_create_map(hid_t file_id, const char *map_name, hid_t key_dtype, hid_t value_dtype)
{
    hid_t map_id = -1;

    TESTING_2("creation of map object")

    if((map_id = H5Mcreate(file_id, map_name, key_dtype, value_dtype, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED(); AT();
        printf("    couldn't create map\n");
        goto error;
    } /* end if */

    if(H5Mclose(map_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Mclose(map_id);
    } H5E_END_TRY;

    return 1;
} /* end test_create_map() */


/*
 * Tests opening a map object
 */
static int
test_open_map(hid_t file_id, const char *map_name)
{
    hid_t map_id = -1;

    TESTING_2("open of map object")

    if((map_id = H5Mopen(file_id, map_name, H5P_DEFAULT)) < 0) {
        H5_FAILED(); AT();
        printf("    couldn't open map\n");
        goto error;
    } /* end if */

    if(H5Mclose(map_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Mclose(map_id);
    } H5E_END_TRY;

    return 1;
} /* end test_open_map() */

/*
 * Tests setting keys in a map object
 */
static int
test_map_set(hid_t file_id, const char *map_name, hid_t key_dtype, hid_t value_dtype)
{
    hid_t map_id = -1;
    int i;

    TESTING_2("map set with keys and values")

    if((map_id = H5Mopen(file_id, map_name, H5P_DEFAULT)) < 0)
        TEST_ERROR

    if(!strcmp(map_name, MAP_INT_INT_NAME)) {
        /* Set the values */
        for(i = 0; i < NUMB_KEYS; i++)
            if(H5Mset(map_id, key_dtype, &int_int_keys[i], value_dtype, &int_int_vals[i], H5P_DEFAULT) < 0) {
                H5_FAILED(); AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */
    } else if(!strcmp(map_name, MAP_ENUM_ENUM_NAME)) {
        /* Set the values */
        for(i = 0; i < NUMB_KEYS; i++)
            if(H5Mset(map_id, key_dtype, &enum_enum_keys[i], value_dtype, &enum_enum_vals[i], H5P_DEFAULT) < 0) {
                H5_FAILED(); AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */
    } else if(!strcmp(map_name, MAP_VL_VL_NAME)) {
        /* Set the values */
        for(i = 0; i < NUMB_KEYS; i++)
            if(H5Mset(map_id, key_dtype, &vl_vl_keys[i], value_dtype, &vl_vl_vals[i], H5P_DEFAULT) < 0) {
                H5_FAILED(); AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */
    } else if(!strcmp(map_name, MAP_COMP_COMP_NAME)) {
        /* Set the values */
        for(i = 0; i < NUMB_KEYS; i++)
            if(H5Mset(map_id, key_dtype, &comp_comp_keys[i], value_dtype, &comp_comp_vals[i], H5P_DEFAULT) < 0) {
                H5_FAILED(); AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */
    } else if(!strcmp(map_name, MAP_LARGE_NAME)) {
        /* Set the values */
        for(i = 0; i < LARGE_NUMB_KEYS; i++)
            if(H5Mset(map_id, key_dtype, &large_int_int_keys[i], value_dtype, &large_int_int_vals[i], H5P_DEFAULT) < 0) {
                H5_FAILED(); AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */
    }

    if(H5Mclose(map_id) < 0)
        TEST_ERROR
    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Mclose(map_id);
    } H5E_END_TRY;

    return 1;
} /* end test_map_set_int_int() */


/*
 * Tests getting keys from a map object
 */
static int
test_map_get(hid_t file_id, const char *map_name, hid_t key_dtype, hid_t value_dtype)
{
    hid_t map_id = -1;
    int i;

    TESTING_2("map get with keys and values")

    if((map_id = H5Mopen(file_id, map_name, H5P_DEFAULT)) < 0)
        TEST_ERROR

    /* Get the values and check that they are correct */
    if(!strcmp(map_name, MAP_INT_INT_NAME)) {
        for(i = 0; i < NUMB_KEYS; i++) {
            if(H5Mget(map_id, key_dtype, &int_int_keys[i], value_dtype, &int_vals_out[i], H5P_DEFAULT) < 0) {
                H5_FAILED(); AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if(int_vals_out[i] != int_int_vals[i]) {
                H5_FAILED(); AT();
                printf("incorrect value returned - index is %d\n", i);
                goto error;
            } /* end if */
        }
    } else if(!strcmp(map_name, MAP_ENUM_ENUM_NAME)) {
        for(i = 0; i < NUMB_KEYS; i++) {
            if(H5Mget(map_id, key_dtype, &enum_enum_keys[i], value_dtype, &enum_vals_out[i], H5P_DEFAULT) < 0) {
                H5_FAILED(); AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if(enum_vals_out[i] != enum_enum_vals[i]) {
                H5_FAILED(); AT();
                printf("incorrect value returned - index is %d\n", i);
                goto error;
            } /* end if */
        }
    } else if(!strcmp(map_name, MAP_VL_VL_NAME)) {
        for(i = 0; i < NUMB_KEYS; i++) {
            if(H5Mget(map_id, key_dtype, &(vl_vl_keys[i]), value_dtype, &(vl_vals_out[i]), H5P_DEFAULT) < 0) {
                H5_FAILED(); AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if(vl_vl_vals[i].len != vl_vals_out[i].len || memcmp(vl_vl_vals[i].p, vl_vals_out[i].p, vl_vl_vals[i].len)) {
                    H5_FAILED(); AT();
                    printf("incorrect value returned\n");
                    goto error;
            }
        } /* end for */
    } else if(!strcmp(map_name, MAP_COMP_COMP_NAME)) {
        for(i = 0; i < NUMB_KEYS; i++) {
            if(H5Mget(map_id, key_dtype, &comp_comp_keys[i], value_dtype, &comp_vals_out[i], H5P_DEFAULT) < 0) {
                H5_FAILED(); AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if(comp_vals_out[i].a != comp_comp_vals[i].a || comp_vals_out[i].b != comp_comp_vals[i].b) {
                H5_FAILED(); AT();
                printf("incorrect value returned - index is %d\n", i);
                goto error;
            } /* end if */
        }
    } else if(!strcmp(map_name, MAP_LARGE_NAME)) {
        for(i = 0; i < LARGE_NUMB_KEYS; i++) {
            if(H5Mget(map_id, key_dtype, &large_int_int_keys[i], value_dtype, &large_int_vals_out[i], H5P_DEFAULT) < 0) {
                H5_FAILED(); AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if(large_int_vals_out[i] != large_int_int_vals[i]) {
                H5_FAILED(); AT();
                printf("incorrect value returned - index is %d\n", i);
                goto error;
            } /* end if */
        }
    }
 
    if(H5Mclose(map_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Mclose(map_id);
    } H5E_END_TRY;

    return 1;
} /* end test_map_get() */

/*
 * Tests getting a non-existent key from a map object
 */
static int
test_map_nonexistent_key(hid_t file_id, const char *map_name, hid_t key_dtype, hid_t value_dtype)
{
    hid_t map_id = -1;
    herr_t error;
    int i;

    TESTING_2("map with a non-existent key")

    if((map_id = H5Mopen(file_id, map_name, H5P_DEFAULT)) < 0)
        TEST_ERROR

    /* Get the values and check that they are correct */
    if(!strcmp(map_name, MAP_INT_INT_NAME)) {
        int non_existent_key, non_existent_value;

        /* Initialize a key that doesn't exist */
        do {
            non_existent_key = rand();
            for(i = 0; i < NUMB_KEYS; i++)
                if(non_existent_key == int_int_keys[i])
                    break;
        } while(i < NUMB_KEYS);

        H5E_BEGIN_TRY {
            error = H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT);
        } H5E_END_TRY;

        if(error >= 0) {
            H5_FAILED(); AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        /* Deleting a non-existent key should succeed but do nothing */
        if(H5Mdelete_key(map_id, key_dtype, &non_existent_key, H5P_DEFAULT) < 0) {
            H5_FAILED(); AT();
            printf("failed with a non-existent key\n");
            goto error;
        } /* end if */
    } else if(!strcmp(map_name, MAP_ENUM_ENUM_NAME)) {
        enum_key_t   non_existent_key = 10;
        enum_value_t non_existent_value;

        H5E_BEGIN_TRY {
            error = H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT);
        } H5E_END_TRY;

        if(error >= 0) {
            H5_FAILED(); AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        /* Deleting a non-existent key should succeed but do nothing */
        if(H5Mdelete_key(map_id, key_dtype, &non_existent_key, H5P_DEFAULT) < 0) {
            H5_FAILED(); AT();
            printf("failed with a non-existent key\n");
            goto error;
        } /* end if */
    } else if(!strcmp(map_name, MAP_VL_VL_NAME)) {
        hvl_t   non_existent_key, non_existent_value;

        /* Initialize non-existent key */
        non_existent_key.p = malloc(NUMB_KEYS*sizeof(short));
        non_existent_key.len = NUMB_KEYS;
        for(i=0; i < NUMB_KEYS; i++)
            ((short *)non_existent_key.p)[i] = i * 10 + 13;

        H5E_BEGIN_TRY {
            error = H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT);
        } H5E_END_TRY;

        if(error >= 0) {
            H5_FAILED(); AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        /* Deleting a non-existent key should succeed but do nothing */
        if(H5Mdelete_key(map_id, key_dtype, &non_existent_key, H5P_DEFAULT) < 0) {
            H5_FAILED(); AT();
            printf("failed with a non-existent key\n");
            goto error;
        } /* end if */
    } else if(!strcmp(map_name, MAP_COMP_COMP_NAME)) {
        compound_t   non_existent_key, non_existent_value;

        non_existent_key.a = random_base - 10;
        non_existent_key.b = (float)(random_base - 100);

        H5E_BEGIN_TRY {
            error = H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT);
        } H5E_END_TRY;

        if(error >= 0) {
            H5_FAILED(); AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        /* Deleting a non-existent key should succeed but do nothing */
        if(H5Mdelete_key(map_id, key_dtype, &non_existent_key, H5P_DEFAULT) < 0) {
            H5_FAILED(); AT();
            printf("failed with a non-existent key\n");
            goto error;
        } /* end if */
    }

    if(H5Mclose(map_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Mclose(map_id);
    } H5E_END_TRY;

    return 1;
} /* end test_map_get() */


/*
 * Tests updating keys from a map object
 */
static int
test_map_update(hid_t file_id, const char *map_name, hid_t key_dtype, hid_t value_dtype)
{
    hid_t map_id = -1;
    int i;

    TESTING_2("map update with keys and values")

    if((map_id = H5Mopen(file_id, map_name, H5P_DEFAULT)) < 0)
        TEST_ERROR

    for(i = 0; i < NUMB_KEYS; i++) {
        enum_enum_keys[i] = i;
        enum_enum_vals[i] = i+1;
    } /* end for */


    /* Update the values and check that they are correct */
    if(!strcmp(map_name, MAP_INT_INT_NAME)) {
        int updated_values[NUMB_KEYS];

        for(i = 0; i < NUMB_KEYS; i++) {
            updated_values[i] = rand();

            if(H5Mset(map_id, key_dtype, &int_int_keys[i], value_dtype, &updated_values[i], H5P_DEFAULT) < 0) {
                H5_FAILED(); AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */

            if(H5Mget(map_id, key_dtype, &int_int_keys[i], value_dtype, &int_vals_out[i], H5P_DEFAULT) < 0) {
                H5_FAILED(); AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if(int_vals_out[i] != updated_values[i]) {
                H5_FAILED(); AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */
        }
    } else if(!strcmp(map_name, MAP_ENUM_ENUM_NAME)) {
        enum_value_t updated_values[NUMB_KEYS];

        for(i = 0; i < NUMB_KEYS; i++) {
            updated_values[i] = NUMB_KEYS + 1 - i;

            if(H5Mset(map_id, key_dtype, &enum_enum_keys[i], value_dtype, &updated_values[i], H5P_DEFAULT) < 0) {
                H5_FAILED(); AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */

            if(H5Mget(map_id, key_dtype, &enum_enum_keys[i], value_dtype, &enum_vals_out[i], H5P_DEFAULT) < 0) {
                H5_FAILED(); AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if(enum_vals_out[i] != updated_values[i]) {
                H5_FAILED(); AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */
        }
    } else if(!strcmp(map_name, MAP_VL_VL_NAME)) {
        hvl_t updated_values[NUMB_KEYS];
        int j;

        for(i = 0; i < NUMB_KEYS; i++) {
            for(j=0; j<(i + NUMB_KEYS); j++) {
                updated_values[i].p = malloc((i + 2 * NUMB_KEYS) * sizeof(int));
                updated_values[i].len = i + 2 * NUMB_KEYS;
            }

            for(j=0; j<(i + 2*NUMB_KEYS); j++)
                ((int *)updated_values[i].p)[j] = rand();

            if(H5Mset(map_id, key_dtype, &vl_vl_keys[i], value_dtype, &updated_values[i], H5P_DEFAULT) < 0) {
                H5_FAILED(); AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */

            if(H5Mget(map_id, key_dtype, &vl_vl_keys[i], value_dtype, &vl_vals_out[i], H5P_DEFAULT) < 0) {
                H5_FAILED(); AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if(updated_values[i].len != vl_vals_out[i].len || memcmp(updated_values[i].p, vl_vals_out[i].p, updated_values[i].len)) {
                    H5_FAILED(); AT();
                    printf("incorrect value returned\n");
                    goto error;
            }
        }
    } else if(!strcmp(map_name, MAP_COMP_COMP_NAME)) {
        compound_t updated_values[NUMB_KEYS];

        for(i = 0; i < NUMB_KEYS; i++) {
            updated_values[i].a = rand();
            updated_values[i].b = (float)rand();

            if(H5Mset(map_id, key_dtype, &comp_comp_keys[i], value_dtype, &updated_values[i], H5P_DEFAULT) < 0) {
                H5_FAILED(); AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */

            if(H5Mget(map_id, key_dtype, &comp_comp_keys[i], value_dtype, &comp_vals_out[i], H5P_DEFAULT) < 0) {
                H5_FAILED(); AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if(comp_vals_out[i].a != updated_values[i].a || comp_vals_out[i].b != updated_values[i].b) {
                H5_FAILED(); AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */
        }
    }

    if(H5Mclose(map_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Mclose(map_id);
    } H5E_END_TRY;

    return 1;
} /* end test_map_update() */


/*
 * Tests checking if keys exist in a map object
 */
static int
test_map_exists(hid_t file_id, const char *map_name, hid_t key_dtype)
{
    hid_t map_id = -1;
    hbool_t exists;
    int i;

    TESTING_2("map exists with the keys and values")

    if((map_id = H5Mopen(file_id, map_name, H5P_DEFAULT)) < 0)
        TEST_ERROR

    if(!strcmp(map_name, MAP_INT_INT_NAME)) {
        int nonexist_key_int;

        for(i = 0; i < NUMB_KEYS; i++) {
            /* Check if the existing keys exist (should all be TRUE) */
            if(H5Mexists(map_id, key_dtype, &int_int_keys[i], &exists, H5P_DEFAULT) < 0) {
                H5_FAILED(); AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if(!exists) {
                H5_FAILED(); AT();
                printf("incorrect value returned: index %d\n", i);
                goto error;
            } /* end if */
        } /* end for */

        /* Initialize a key that doesn't exist */
        do {
            nonexist_key_int = rand();
            for(i = 0; i < NUMB_KEYS; i++)
                if(nonexist_key_int == int_int_keys[i])
                    break;
        } while(i < NUMB_KEYS);

        /* Check if the nonexisting key exists (should be FALSE) */
        if(H5Mexists(map_id, key_dtype, &nonexist_key_int, &exists, H5P_DEFAULT) < 0) {
            H5_FAILED(); AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if(exists) {
            H5_FAILED(); AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */
    } else if(!strcmp(map_name, MAP_ENUM_ENUM_NAME)) {
        enum_key_t nonexist_key_enum;

        for(i = 0; i < NUMB_KEYS; i++) {
            if(H5Mexists(map_id, key_dtype, &enum_enum_keys[i], &exists, H5P_DEFAULT) < 0) {
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

        nonexist_key_enum = NUMB_KEYS + 1;

        /* Check if the nonexisting key exists (should be FALSE) */
        if(H5Mexists(map_id, key_dtype, &nonexist_key_enum, &exists, H5P_DEFAULT) < 0) {
            H5_FAILED(); AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if(exists) {
            H5_FAILED(); AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */
    } else if(!strcmp(map_name, MAP_VL_VL_NAME)) {
        hvl_t nonexist_key_vl;

        for(i = 0; i < NUMB_KEYS; i++) {
            if(H5Mexists(map_id, key_dtype, &(vl_vl_keys[i]), &exists, H5P_DEFAULT) < 0) {
                H5_FAILED(); AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if(!exists) {
                H5_FAILED(); AT();
                printf("incorrect value returned: %d\n", i);
                goto error;
            } /* end if */
        } /* end for */

        /* Initialize non-existent key */
        nonexist_key_vl.p = malloc(10*sizeof(short));
        nonexist_key_vl.len = 10;
        for(i=0; i<10; i++)
            ((short *)nonexist_key_vl.p)[i] = 100 + i;

        /* Check if the nonexisting key exists (should be FALSE) */
        if(H5Mexists(map_id, key_dtype, &nonexist_key_vl, &exists, H5P_DEFAULT) < 0) {
            H5_FAILED(); AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if(exists) {
            H5_FAILED(); AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */
    } else if(!strcmp(map_name, MAP_COMP_COMP_NAME)) {
        compound_t nonexist_key_comp;

        for(i = 0; i < NUMB_KEYS; i++) {
            if(H5Mexists(map_id, key_dtype, &comp_comp_keys[i], &exists, H5P_DEFAULT) < 0) {
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

        nonexist_key_comp.a = random_base - 1;
        nonexist_key_comp.b = (float)(random_base - 1);

        /* Check if the nonexisting key exists (should be FALSE) */
        if(H5Mexists(map_id, key_dtype, &nonexist_key_comp, &exists, H5P_DEFAULT) < 0) {
            H5_FAILED(); AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if(exists) {
            H5_FAILED(); AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */
    } else if(!strcmp(map_name, MAP_LARGE_NAME)) {
        for(i = 0; i < LARGE_NUMB_KEYS; i++) {
            /* Check if the existing keys exist (should all be TRUE) */
            if(H5Mexists(map_id, key_dtype, &large_int_int_keys[i], &exists, H5P_DEFAULT) < 0) {
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
    }

    if(H5Mclose(map_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Mclose(map_id);
    } H5E_END_TRY;

    return 1;
} /* end test_map_exists() */

/*
 * Tests iterating over all keys in a map object 
 */
typedef struct {
    int *keys_visited;
    int ncalls;
    int stop_at;
    char *map_name;
} iterate_ud_t;

static herr_t
map_iterate_cb(hid_t map_id, const void *_key, void *_iterate_ud)
{
    iterate_ud_t *iterate_ud = (iterate_ud_t *)_iterate_ud;
    int i;

    /* Check parameters */
    if(!_key) {
        H5_FAILED(); AT();
        printf("key is NULL\n");
        goto error;
    } /* end if */
    if(!iterate_ud) {
        H5_FAILED(); AT();
        printf("op_data is NULL\n");
        goto error;
    } /* end if */
    if(!iterate_ud->map_name) {
        H5_FAILED(); AT();
        printf("op_data is NULL\n");
        goto error;
    } /* end if */

    /* Mark key visited */
    if(!strcmp(iterate_ud->map_name, MAP_INT_INT_NAME)) {
        for(i = 0; i < NUMB_KEYS; i++) {
            if(int_int_keys[i] == *((const int *)_key)) {
                iterate_ud->keys_visited[i]++;
                break;
            } /* end if */
        }

        if(i == NUMB_KEYS) {
            H5_FAILED(); AT();
            printf("key not found: %d among ", *((const int *)_key));
            for(i = 0; i < NUMB_KEYS; i++)
                printf("%d, ", int_int_keys[i]);
            printf("\n");
            goto error;
        } /* end if */
    } else if(!strcmp(iterate_ud->map_name, MAP_ENUM_ENUM_NAME)) {
        for(i = 0; i < NUMB_KEYS; i++) {
            if(enum_enum_keys[i] == *((const enum_key_t *)_key)) {
                iterate_ud->keys_visited[i]++;
                break;
            } /* end if */
        }

        if(i == NUMB_KEYS) {
            H5_FAILED(); AT();
            printf("key not found\n");
            goto error;
        } /* end if */
    } else if(!strcmp(iterate_ud->map_name, MAP_VL_VL_NAME)) {
        for(i = 0; i < NUMB_KEYS; i++) {
            if(vl_vl_keys[i].len == ((const hvl_t *)_key)->len && !memcmp(vl_vl_keys[i].p, ((const hvl_t *)_key)->p, vl_vl_keys[i].len)) {
                iterate_ud->keys_visited[i]++;
                break;
            } /* end if */
        }

        if(i == NUMB_KEYS) {
            H5_FAILED(); AT();
            printf("key not found\n");
            goto error;
        } /* end if */
    } else if(!strcmp(iterate_ud->map_name, MAP_COMP_COMP_NAME)) {
        for(i = 0; i < NUMB_KEYS; i++) {
            if(comp_comp_keys[i].a == ((const compound_t *)_key)->a && comp_comp_keys[i].b == ((const compound_t *)_key)->b) {
                iterate_ud->keys_visited[i]++;
                break;
            } /* end if */
        }

        if(i == NUMB_KEYS) {
            H5_FAILED(); AT();
            printf("key not found\n");
            goto error;
        } /* end if */
    } else if(!strcmp(iterate_ud->map_name, MAP_LARGE_NAME)) {
        for(i = 0; i < LARGE_NUMB_KEYS; i++) {
            if(large_int_int_keys[i] == *((const int *)_key)) {
                iterate_ud->keys_visited[i]++;
                break;
            } /* end if */
        }

        if(i == LARGE_NUMB_KEYS) {
            H5_FAILED(); AT();
            printf("key not found\n");
            goto error;
        } /* end if */
    }

    /* Check for short circuit */
    if(++iterate_ud->ncalls == iterate_ud->stop_at)
        return 1;

    return 0;

error:
    return -1;
} /* end map_iterate_cb */

static int
test_map_iterate(hid_t file_id, const char *map_name, hid_t key_dtype)
{
    hid_t map_id = -1;
    iterate_ud_t iterate_ud;
    hsize_t idx;
    int nkeys;
    int ret;
    int i;

    TESTING_2("iterating over keys in map")

    if((map_id = H5Mopen(file_id, map_name, H5P_DEFAULT)) < 0)
        TEST_ERROR

    /* Reset iterate_cb */
    memset(&iterate_ud, 0, sizeof(iterate_ud));

    /* Copy the map name to the struct */
    iterate_ud.map_name = strdup(map_name);

    if(!strcmp(map_name, MAP_LARGE_NAME))
        iterate_ud.keys_visited = calloc(LARGE_NUMB_KEYS, sizeof(int));
    else
        iterate_ud.keys_visited = calloc(NUMB_KEYS, sizeof(int));

    /* Iterate over all keys */
    iterate_ud.stop_at = -1;
    idx = 0;
    if((ret = H5Miterate(map_id, &idx, key_dtype, map_iterate_cb, &iterate_ud, H5P_DEFAULT)) < 0) {
        H5_FAILED(); AT();
        printf("failed to iterate over map\n");
        goto error;
    } /* end if */
    if(ret != 0) {
        H5_FAILED(); AT();
        printf("incorrect return code from H5Miterate\n");
        goto error;
    } /* end if */

    /* Check that all keys were visited exactly once */
    if(!strcmp(map_name, MAP_LARGE_NAME)) {
        if(idx != (hsize_t)LARGE_NUMB_KEYS){
            H5_FAILED(); AT();
            printf("incorrect value of idx after H5Miterate\n");
            goto error;
        } /* end if */

        for(i = 0; i < LARGE_NUMB_KEYS; i++)
            if(iterate_ud.keys_visited[i] != 1) {
                H5_FAILED(); AT();
                printf("key visited an incorrect number of times\n");
                goto error;
            } /* end if */
    } else {
        if(idx != (hsize_t)NUMB_KEYS){
            H5_FAILED(); AT();
            printf("incorrect value of idx after H5Miterate\n");
            goto error;
        } /* end if */

        for(i = 0; i < NUMB_KEYS; i++)
            if(iterate_ud.keys_visited[i] != 1) {
                H5_FAILED(); AT();
                printf("key visited an incorrect number of times\n");
                goto error;
            } /* end if */
    }

    /* Reset iterate_cb */
    if(iterate_ud.map_name) {
        free(iterate_ud.map_name);
        iterate_ud.map_name = NULL;
    }

    if(iterate_ud.keys_visited) {
        free(iterate_ud.keys_visited);
        iterate_ud.keys_visited = NULL;
    }

    memset(&iterate_ud, 0, sizeof(iterate_ud));

    /* Copy the map name to the struct */
    iterate_ud.map_name = strdup(map_name);

    if(!strcmp(map_name, MAP_LARGE_NAME))
        iterate_ud.keys_visited = calloc(LARGE_NUMB_KEYS, sizeof(int));
    else
        iterate_ud.keys_visited = calloc(NUMB_KEYS, sizeof(int));

    /* Iterate but stop after the second key */
    iterate_ud.stop_at = 2;
    idx = 0;
    if((ret = H5Miterate(map_id, &idx, key_dtype, map_iterate_cb, &iterate_ud, H5P_DEFAULT)) < 0) {
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
    if(!strcmp(map_name, MAP_LARGE_NAME)) {
        for(i = 0; i < LARGE_NUMB_KEYS; i++) {
            if(iterate_ud.keys_visited[i] > 1) {
                H5_FAILED(); AT();
                printf("key visited an incorrect number of times\n");
                goto error;
            } /* end if */
            nkeys += iterate_ud.keys_visited[i];
        }
    } else {
        for(i = 0; i < NUMB_KEYS; i++) {
            if(iterate_ud.keys_visited[i] > 1) {
                H5_FAILED(); AT();
                printf("key visited an incorrect number of times\n");
                goto error;
            } /* end if */
            nkeys += iterate_ud.keys_visited[i];
        }
    }

    if(nkeys != 2) {
        H5_FAILED(); AT();
        printf("incorrect number of keys visited\n");
        goto error;
    } /* end if */

    if(iterate_ud.map_name) {
        free(iterate_ud.map_name);
        iterate_ud.map_name = NULL;
    }

    if(iterate_ud.keys_visited) {
        free(iterate_ud.keys_visited);
        iterate_ud.keys_visited = NULL;
    }
    if(H5Mclose(map_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Mclose(map_id);
    } H5E_END_TRY;

    if(iterate_ud.map_name)
        free(iterate_ud.map_name);

    if(iterate_ud.keys_visited)
        free(iterate_ud.keys_visited);

    return 1;
} /* end test_map_iterate() */

/*
 * Tests checking if an entry can be removed in a map object
 */
static int
test_map_delete_key(hid_t file_id, const char *map_name, hid_t key_dtype)
{
    hid_t map_id = -1;
    hbool_t exists;
    int i;

    TESTING_2("removing an entry by the key")

    if((map_id = H5Mopen(file_id, map_name, H5P_DEFAULT)) < 0)
        TEST_ERROR

    /* Check if the existing keys exist (should all be TRUE) */
    if(!strcmp(map_name, MAP_INT_INT_NAME)) {
        /* Delete the first entry */
        if(H5Mdelete_key(map_id, key_dtype, &int_int_keys[0], H5P_DEFAULT) < 0) {
            H5_FAILED(); AT();
            printf("failed to remove an entry by the key\n");
            goto error;
        } /* end if */

        /* Check if the deleted key still exists (should be FALSE) */
        if(H5Mexists(map_id, key_dtype, &int_int_keys[0], &exists, H5P_DEFAULT) < 0) {
            H5_FAILED(); AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if(exists) {
            H5_FAILED(); AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */

        /* Check the rest of entries still exist */
        for(i = 1; i < NUMB_KEYS; i++) {
            if(H5Mexists(map_id, key_dtype, &int_int_keys[i], &exists, H5P_DEFAULT) < 0) {
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
    } else if(!strcmp(map_name, MAP_ENUM_ENUM_NAME)) {
        /* Delete the first entry */
        if(H5Mdelete_key(map_id, key_dtype, &enum_enum_keys[0], H5P_DEFAULT) < 0) {
            H5_FAILED(); AT();
            printf("failed to remove an entry by the key\n");
            goto error;
        } /* end if */

        /* Check if the deleted key still exists (should be FALSE) */
        if(H5Mexists(map_id, key_dtype, &enum_enum_keys[0], &exists, H5P_DEFAULT) < 0) {
            H5_FAILED(); AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if(exists) {
            H5_FAILED(); AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */

        /* Check the rest of entries still exist */
        for(i = 1; i < NUMB_KEYS; i++) {
            if(H5Mexists(map_id, key_dtype, &enum_enum_keys[i], &exists, H5P_DEFAULT) < 0) {
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
    } else if(!strcmp(map_name, MAP_VL_VL_NAME)) {
        /* Delete the first entry */
        if(H5Mdelete_key(map_id, key_dtype, &(vl_vl_keys[0]), H5P_DEFAULT) < 0) {
            H5_FAILED(); AT();
            printf("failed to remove an entry by the key\n");
            goto error;
        } /* end if */

        /* Check if the nonexisting key exists (should be FALSE) */
        if(H5Mexists(map_id, key_dtype, &(vl_vl_keys[0]), &exists, H5P_DEFAULT) < 0) {
            H5_FAILED(); AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if(exists) {
            H5_FAILED(); AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */

        /* Check the rest of entries still exist */
        for(i = 1; i < NUMB_KEYS; i++) {
            if(H5Mexists(map_id, key_dtype, &(vl_vl_keys[i]), &exists, H5P_DEFAULT) < 0) {
                H5_FAILED(); AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if(!exists) {
                H5_FAILED(); AT();
                printf("incorrect value returned: %d\n", i);
                goto error;
            } /* end if */
        } /* end for */
    } else if(!strcmp(map_name, MAP_COMP_COMP_NAME)) {
        /* Delete the first entry */
        if(H5Mdelete_key(map_id, key_dtype, &comp_comp_keys[0], H5P_DEFAULT) < 0) {
            H5_FAILED(); AT();
            printf("failed to remove an entry by the key\n");
            goto error;
        } /* end if */

        /* Check if the deleted key still exists (should be FALSE) */
        if(H5Mexists(map_id, key_dtype, &comp_comp_keys[0], &exists, H5P_DEFAULT) < 0) {
            H5_FAILED(); AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if(exists) {
            H5_FAILED(); AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */

        /* Check the rest of entries still exist */
        for(i = 1; i < NUMB_KEYS; i++) {
            if(H5Mexists(map_id, key_dtype, &comp_comp_keys[i], &exists, H5P_DEFAULT) < 0) {
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
    } else if(!strcmp(map_name, MAP_LARGE_NAME)) {
        /* Delete the first entry */
        if(H5Mdelete_key(map_id, key_dtype, &large_int_int_keys[0], H5P_DEFAULT) < 0) {
            H5_FAILED(); AT();
            printf("failed to remove an entry by the key\n");
            goto error;
        } /* end if */

        /* Check if the deleted key still exists (should be FALSE) */
        if(H5Mexists(map_id, key_dtype, &large_int_int_keys[0], &exists, H5P_DEFAULT) < 0) {
            H5_FAILED(); AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if(exists) {
            H5_FAILED(); AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */

        /* Check the rest of entries still exist */
        for(i = 1; i < LARGE_NUMB_KEYS; i++) {
            if(H5Mexists(map_id, key_dtype, &large_int_int_keys[i], &exists, H5P_DEFAULT) < 0) {
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
    }

    if(H5Mclose(map_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Mclose(map_id);
    } H5E_END_TRY;

    return 1;
} /* end test_map_exists() */


static int 
test_integer(hid_t file_id)
{
    int     i, j;
    int     nerrors = 0;

    TESTING("integer as the datatype of keys and values"); HDputs("");

    /* Generate random keys and values */
    for(i = 0; i < NUMB_KEYS; i++) {
        do {
            int_int_keys[i] = rand();
            for(j = 0; j < i; j++)
                if(int_int_keys[i] == int_int_keys[j])
                    break;
        } while(j < i);
        int_int_vals[i] = rand();
    } /* end for */

    nerrors += test_create_map(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT);
    nerrors += test_open_map(file_id, MAP_INT_INT_NAME);
    nerrors += test_map_set(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT);
    nerrors += test_map_get(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT);
    nerrors += test_map_exists(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT);
    nerrors += test_map_iterate(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT);
    nerrors += test_map_update(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT);
    nerrors += test_map_nonexistent_key(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT);
    nerrors += test_map_delete_key(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT);

    return nerrors;
}

static int 
test_enum(hid_t file_id)
{
    hid_t key_dtype_id = -1, value_dtype_id = -1;
    enum_value_t val;
    enum_key_t   key;
    int     nerrors = 0;
    int     i;

    TESTING("enum as the datatype of keys and values"); HDputs("");

    if((value_dtype_id = H5Tcreate(H5T_ENUM, sizeof(enum_value_t))) < 0) goto error;
    if(H5Tenum_insert(value_dtype_id, "RED",   CPTR(val, RED  )) < 0) goto error;
    if(H5Tenum_insert(value_dtype_id, "GREEN", CPTR(val, GREEN)) < 0) goto error; 
    if(H5Tenum_insert(value_dtype_id, "BLUE",  CPTR(val, BLUE )) < 0) goto error; 
    if(H5Tenum_insert(value_dtype_id, "WHITE", CPTR(val, WHITE)) < 0) goto error; 
    if(H5Tenum_insert(value_dtype_id, "BLACK", CPTR(val, BLACK)) < 0) goto error; 
    if(H5Tcommit2(file_id, "value_datatype", value_dtype_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) < 0) goto error;

    if((key_dtype_id = H5Tcreate(H5T_ENUM, sizeof(enum_value_t))) < 0) goto error;
    if(H5Tenum_insert(key_dtype_id, "ONE",   CPTR(key, ONE  )) < 0) goto error;
    if(H5Tenum_insert(key_dtype_id, "TWO",   CPTR(key, TWO  )) < 0) goto error; 
    if(H5Tenum_insert(key_dtype_id, "THREE", CPTR(key, THREE)) < 0) goto error; 
    if(H5Tenum_insert(key_dtype_id, "FOUR",  CPTR(key,  FOUR )) < 0) goto error; 
    if(H5Tenum_insert(key_dtype_id, "FIVE",  CPTR(key,  FIVE )) < 0) goto error; 
    if(H5Tcommit2(file_id, "key_datatype", key_dtype_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) < 0) goto error;

    /* Generate enum keys and values */
    for(i = 0; i < NUMB_KEYS; i++) {
        enum_enum_keys[i] = i;
        enum_enum_vals[i] = i+1;
    } /* end for */

    nerrors += test_create_map(file_id, MAP_ENUM_ENUM_NAME, key_dtype_id, value_dtype_id);
    nerrors += test_open_map(file_id, MAP_ENUM_ENUM_NAME);
    nerrors += test_map_set(file_id, MAP_ENUM_ENUM_NAME, key_dtype_id, value_dtype_id);
    nerrors += test_map_get(file_id, MAP_ENUM_ENUM_NAME, key_dtype_id, value_dtype_id);
    nerrors += test_map_exists(file_id, MAP_ENUM_ENUM_NAME, key_dtype_id);
    nerrors += test_map_iterate(file_id, MAP_ENUM_ENUM_NAME, key_dtype_id);
    nerrors += test_map_update(file_id, MAP_ENUM_ENUM_NAME, key_dtype_id, value_dtype_id);
    nerrors += test_map_nonexistent_key(file_id, MAP_ENUM_ENUM_NAME, key_dtype_id, value_dtype_id);
    nerrors += test_map_delete_key(file_id, MAP_ENUM_ENUM_NAME, key_dtype_id);

    if(H5Tclose(key_dtype_id) < 0) goto error;
    if(H5Tclose(value_dtype_id) < 0) goto error;

    return nerrors;

error:
    return 1;
}

static int 
test_vl(hid_t file_id)
{
    hid_t   key_dtype_id = -1, value_dtype_id = -1;
    int     nerrors = 0;
    int     i, j;

    TESTING("variable-length as the datatype of keys and values"); HDputs("");

    key_dtype_id = H5Tvlen_create(H5T_NATIVE_SHORT);
    value_dtype_id = H5Tvlen_create(H5T_NATIVE_INT);

    /* Allocate and initialize VL data to keys and values */
    for(i=0; i<NUMB_KEYS; i++) {
        vl_vl_keys[i].p = malloc((i+1)*sizeof(short));
        vl_vl_keys[i].len = i+1;
        for(j=0; j<(i+1); j++)
            ((short *)vl_vl_keys[i].p)[j] = i*10+j+7;

        vl_vl_vals[i].p = malloc((i + NUMB_KEYS)*sizeof(int));
        vl_vl_vals[i].len = i + NUMB_KEYS;
        for(j=0; j<(i + NUMB_KEYS); j++)
            ((int *)vl_vl_vals[i].p)[j] = random_base + j;  
    } /* end for */

    nerrors += test_create_map(file_id, MAP_VL_VL_NAME, key_dtype_id, value_dtype_id);
    nerrors += test_open_map(file_id, MAP_VL_VL_NAME);
    nerrors += test_map_set(file_id, MAP_VL_VL_NAME, key_dtype_id, value_dtype_id);
    nerrors += test_map_get(file_id, MAP_VL_VL_NAME, key_dtype_id, value_dtype_id);
    nerrors += test_map_exists(file_id, MAP_VL_VL_NAME, key_dtype_id);
    nerrors += test_map_iterate(file_id, MAP_VL_VL_NAME, key_dtype_id);
    nerrors += test_map_update(file_id, MAP_VL_VL_NAME, key_dtype_id, value_dtype_id);
    nerrors += test_map_nonexistent_key(file_id, MAP_VL_VL_NAME, key_dtype_id, value_dtype_id);
    nerrors += test_map_delete_key(file_id, MAP_VL_VL_NAME, key_dtype_id);

    if(H5Tclose(key_dtype_id) < 0) goto error;
    if(H5Tclose(value_dtype_id) < 0) goto error;

    return nerrors;

error:
    return 1;
}

static int
test_compound(hid_t file_id)
{
    hid_t   dtype_id = -1;
    int     i;
    int     nerrors = 0;

    TESTING("compound as the datatype of keys and values"); HDputs("");

    dtype_id = H5Tcreate (H5T_COMPOUND, sizeof(compound_t));
    H5Tinsert(dtype_id, "a_name", HOFFSET(compound_t, a), H5T_NATIVE_INT);
    H5Tinsert(dtype_id, "b_name", HOFFSET(compound_t, b), H5T_NATIVE_FLOAT);

    /* Generate random keys and values */
    for(i = 0; i < NUMB_KEYS; i++) {
        comp_comp_keys[i].a = random_base + i;
        comp_comp_keys[i].b = (float)(random_base + i*i);
        comp_comp_vals[i].a = rand();
        comp_comp_vals[i].b = (float)rand();
    } /* end for */

    nerrors += test_create_map(file_id, MAP_COMP_COMP_NAME, dtype_id, dtype_id);
    nerrors += test_open_map(file_id, MAP_COMP_COMP_NAME);
    nerrors += test_map_set(file_id, MAP_COMP_COMP_NAME, dtype_id, dtype_id);
    nerrors += test_map_get(file_id, MAP_COMP_COMP_NAME, dtype_id, dtype_id);
    nerrors += test_map_exists(file_id, MAP_COMP_COMP_NAME, dtype_id);
    nerrors += test_map_iterate(file_id, MAP_COMP_COMP_NAME, dtype_id);
    nerrors += test_map_update(file_id, MAP_COMP_COMP_NAME, dtype_id, dtype_id);
    nerrors += test_map_nonexistent_key(file_id, MAP_COMP_COMP_NAME, dtype_id, dtype_id);
    nerrors += test_map_delete_key(file_id, MAP_COMP_COMP_NAME, dtype_id);

    if(H5Tclose(dtype_id) < 0) goto error;

    return nerrors;

error:
    return 1;
}

static int
test_many_entries(hid_t file_id)
{
    int     i;
    int     nerrors = 0;

    TESTING("large number of entries of keys and values"); HDputs("");

    /* Generate random keys and values */
    for(i = 0; i < LARGE_NUMB_KEYS; i++) {
        large_int_int_keys[i] = (rand() % (256 * 256 * 256 * 32 / LARGE_NUMB_KEYS)) * LARGE_NUMB_KEYS + i;
        large_int_int_vals[i] = rand();
    } /* end for */

    nerrors += test_create_map(file_id, MAP_LARGE_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT);
    nerrors += test_open_map(file_id, MAP_LARGE_NAME);
    nerrors += test_map_set(file_id, MAP_LARGE_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT);
    nerrors += test_map_get(file_id, MAP_LARGE_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT);
    nerrors += test_map_exists(file_id, MAP_LARGE_NAME, H5T_NATIVE_INT);
    nerrors += test_map_iterate(file_id, MAP_LARGE_NAME, H5T_NATIVE_INT);
    nerrors += test_map_delete_key(file_id, MAP_LARGE_NAME, H5T_NATIVE_INT);

    return nerrors;
}

/*
 * Tests creating many map objects
 */
static int
test_many_maps(hid_t file_id)
{
    hid_t map_id = -1;
    char  map_name[32];
    int i;

    TESTING("large number of map objects")

    for(i = 0; i < LARGE_NUMB_MAPS; i++) {
        sprintf(map_name, "map_%d", i);
        if((map_id = H5Mcreate(file_id, map_name, H5T_NATIVE_INT, H5T_NATIVE_INT, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
            H5_FAILED(); AT();
            printf("    couldn't create map\n");
            goto error;
        } /* end if */

        if(H5Mclose(map_id) < 0)
            TEST_ERROR
    }

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Mclose(map_id);
    } H5E_END_TRY;

    return 1;
} /* end test_many_maps() */

/*
 * Tests opening a non-existent map object
 */
static int
test_nonexistent_map(hid_t file_id)
{
    hid_t map_id = -1;

    TESTING("opening a non-existent map object")

    H5E_BEGIN_TRY {
        map_id = H5Mopen(file_id, MAP_NONEXISTENT_MAP, H5P_DEFAULT);
    } H5E_END_TRY;

    if(map_id >= 0) {
        H5_FAILED(); AT();
        printf("    shouldn't open the nonexistent map\n");
        goto error;
    } /* end if */

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Mclose(map_id);
    } H5E_END_TRY;

    return 1;
} /* end test_nonexistent_map() */


/*
 * main function
 */
int
main( int argc, char** argv )
{
    hid_t fapl_id = -1, file_id = -1;
    int     nerrors = 0;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

    srand((unsigned) time(NULL));
    random_base = rand();

    if((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0) {
        nerrors++;
        goto error;
    }

    if(H5Pset_all_coll_metadata_ops(fapl_id, true) < 0) {
        nerrors++;
        goto error;
    }

    if((file_id = H5Fcreate(FILENAME, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0) {
        nerrors++;
        goto error;
    }

    nerrors += test_integer(file_id);
    nerrors += test_enum(file_id);
#ifdef TMP
    nerrors += test_vl(file_id);
#endif
    nerrors += test_compound(file_id);
    nerrors += test_many_entries(file_id);
    nerrors += test_many_maps(file_id);
    nerrors += test_nonexistent_map(file_id);

    if(H5Pclose(fapl_id) < 0) {
        nerrors++;
        goto error;
    }

    if(H5Fclose(file_id) < 0) {
        nerrors++;
        goto error;
    }

    if (nerrors) goto error;

    if (MAINPROCESS) puts("All DAOS Map tests passed");

    MPI_Finalize();

    return 0;

error:
    if (MAINPROCESS) printf("*** %d TEST%s FAILED ***\n", nerrors, (!nerrors || nerrors > 1) ? "S" : "");

    MPI_Finalize();

    return 1;
} /* end main() */

