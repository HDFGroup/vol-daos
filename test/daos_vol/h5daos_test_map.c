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
#define NUMB_KEYS               4   /* Don't set this too high because this test uses linear search */

#define MAP_INT_INT_NAME        "map_int_int"
#define MAP_ENUM_ENUM_NAME      "map_enum_enum"
#define MAP_VL_VL_NAME          "map_vl_vl"

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
} /* end test_create_map() */


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
    int i, j;

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
                    /*printf("incorrect value returned - index is [%d, %d], vl_vl_vals.len = %d, vl_vl_vals = %d, vl_vals_out[i].len = %d, vl_vals_out = %d\n", i, j, 
                        vl_vl_vals[i].len, ((short *)vl_vl_vals[i].p)[j], vl_vals_out[i].len, ((short *)vl_vals_out[i].p)[j]); */
                    goto error;
                }
/*
            for(j = 0; j < vl_vl_vals[i].len; j++)
                if(((short *)vl_vl_vals[i].p)[j] != ((short *)vl_vals_out[i].p)[j]) {
                    H5_FAILED(); AT();
                    printf("incorrect value returned - index is [%d, %d], vl_vl_vals.len = %d, vl_vl_vals = %d, vl_vals_out[i].len = %d, vl_vals_out = %d\n", i, j, 
                        vl_vl_vals[i].len, ((short *)vl_vl_vals[i].p)[j], vl_vals_out[i].len, ((short *)vl_vals_out[i].p)[j]);
                    goto error;
                }
 */
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
} /* end test_map_get() */


/*
 * Tests checking if keys exist in a map object
 */
static int
test_map_exists(hid_t file_id, const char *map_name, hid_t key_dtype)
{
    hid_t map_id = -1;
    hbool_t exists;
    int nonexist_key_int;
    enum_key_t nonexist_key_enum;
    hvl_t nonexist_key_vl;
    int i;

    TESTING_2("map exists with the keys and values")

    if((map_id = H5Mopen(file_id, map_name, H5P_DEFAULT)) < 0)
        TEST_ERROR

    /* Check if the existing keys exist (should all be TRUE) */
    if(!strcmp(map_name, MAP_INT_INT_NAME)) {
        for(i = 0; i < NUMB_KEYS; i++) {
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

        nonexist_key_int = random_base - 1;

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
    int keys_visited[NUMB_KEYS];
    int ncalls;
    int stop_at;
    H5T_class_t key_dtype_class;
} iterate_ud_t;

static herr_t
map_iterate_cb(hid_t map_id, const void *_key, void *_iterate_ud)
{
    iterate_ud_t *iterate_ud = (iterate_ud_t *)_iterate_ud;
    int i, j;

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

    /* Mark key visited */
    for(i = 0; i < NUMB_KEYS; i++) {
        if(iterate_ud->key_dtype_class == H5T_INTEGER) {
            if(int_int_keys[i] == *((const int *)_key)) {
                iterate_ud->keys_visited[i]++;
                break;
            } /* end if */
        } else if(iterate_ud->key_dtype_class == H5T_ENUM) {
            if(enum_enum_keys[i] == *((const enum_key_t *)_key)) {
                iterate_ud->keys_visited[i]++;
                break;
            } /* end if */
        } else if(iterate_ud->key_dtype_class == H5T_VLEN) {
            if(vl_vl_keys[i].len == ((const hvl_t *)_key)->len && !memcmp(vl_vl_keys[i].p, ((const hvl_t *)_key)->p, vl_vl_keys[i].len)) {
                iterate_ud->keys_visited[i]++;
                break;
            } /* end if */
        }
    }

    if(i == NUMB_KEYS) {
        H5_FAILED(); AT();
        //printf("key not found: %u\n", *((const enum_key_t *)_key));
        printf("key not found\n");
        goto error;
    } /* end if */

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

    /* Pass in the datatype class of the key */
    if(!strcmp(map_name, MAP_INT_INT_NAME))
        iterate_ud.key_dtype_class = H5T_INTEGER;
    else if(!strcmp(map_name, MAP_ENUM_ENUM_NAME))
        iterate_ud.key_dtype_class = H5T_ENUM;
    else if(!strcmp(map_name, MAP_VL_VL_NAME))
        iterate_ud.key_dtype_class = H5T_VLEN;

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
    if(idx != (hsize_t)NUMB_KEYS){
        H5_FAILED(); AT();
        printf("incorrect value of idx after H5Miterate\n");
        goto error;
    } /* end if */

    /* Check that all keys were visited exactly once */
    for(i = 0; i < NUMB_KEYS; i++)
        if(iterate_ud.keys_visited[i] != 1) {
            H5_FAILED(); AT();
            printf("key visited an incorrect number of times\n");
            goto error;
        } /* end if */

    /* Reset iterate_cb */
    memset(&iterate_ud, 0, sizeof(iterate_ud));

    /* Pass in the datatype class of the key */
    if(!strcmp(map_name, MAP_INT_INT_NAME))
        iterate_ud.key_dtype_class = H5T_INTEGER;
    else if(!strcmp(map_name, MAP_ENUM_ENUM_NAME))
        iterate_ud.key_dtype_class = H5T_ENUM;
    else if(!strcmp(map_name, MAP_VL_VL_NAME))
        iterate_ud.key_dtype_class = H5T_VLEN;

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
    for(i = 0; i < NUMB_KEYS; i++) {
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

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Mclose(map_id);
    } H5E_END_TRY;

    return 1;
} /* end test_map_iterate() */

static int 
test_integer(hid_t file_id)
{
    int     i;
    int     nerrors = 0;

    TESTING("integer as the datatype of keys and values"); HDputs("");

    /* Generate random keys and values */
    for(i = 0; i < NUMB_KEYS; i++) {
        int_int_keys[i] = random_base + i;
        int_int_vals[i] = rand();
    } /* end for */

    nerrors += test_create_map(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT);
    nerrors += test_open_map(file_id, MAP_INT_INT_NAME);
    nerrors += test_map_set(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT);
    nerrors += test_map_get(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT);
    nerrors += test_map_exists(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT);
    nerrors += test_map_iterate(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT);

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

    if(H5Tclose(key_dtype_id) < 0) goto error;
    if(H5Tclose(value_dtype_id) < 0) goto error;

    return nerrors;

error:
    return 1;
}

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
    nerrors += test_vl(file_id);

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

