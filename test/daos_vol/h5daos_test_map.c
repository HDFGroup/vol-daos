/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

/**
 * Purpose: Tests H5M (Map API) interface
 */

#include "h5daos_test.h"

#include "daos_vol.h"

/*
 * Definitions
 */
#define TRUE  1
#define FALSE 0

#define FILENAME        "h5daos_test_map.h5"
#define NUMB_KEYS       4 /* Don't set this too high because this test uses linear search */
#define LARGE_NUMB_KEYS 1024
#define LARGE_NUMB_MAPS 128

#define MAP_INT_INT_NAME       "map_int_int"
#define MAP_ENUM_ENUM_NAME     "map_enum_enum"
#define MAP_VL_VL_NAME         "map_vl_vl"
#define MAP_VLS_VLS_NAME       "map_vls_vls"
#define MAP_COMP_COMP_NAME     "map_comp_comp"
#define MAP_NESTED_COMP_NAME   "map_nested_comp"
#define MAP_SIMPLE_TCONV1_NAME "map_simple_tconv1"
#define MAP_SIMPLE_TCONV2_NAME "map_simple_tconv2"
#define MAP_VL_TCONV1_NAME     "map_vl_tconv1"
#define MAP_VL_TCONV2_NAME     "map_vl_tconv2"
#define MAP_MANY_ENTRIES_NAME  "map_many_entries"
#define MAP_NONEXISTENT_MAP    "map_nonexistent"

#define CPTR(VAR, CONST) ((VAR) = (CONST), &(VAR))

#define FLOAT_EQUAL(VAR1, VAR2) ((((VAR1) - (VAR2)) < 0.001) && (((VAR1) - (VAR2)) > -0.001))

#define STRCMP_NULL(A, B) (!(A) != !(B) || ((A) && strcmp(A, B)))

#define VLCMP(A, B, TYPE) ((A).len != (B).len || memcmp((A).p, (B).p, (A).len * sizeof(TYPE)))

#define VL_FLOAT_CMP(A, B, A_TYPE, B_TYPE, RESULT)                                                           \
    {                                                                                                        \
        if ((A).len != (B).len)                                                                              \
            RESULT = 1;                                                                                      \
        else {                                                                                               \
            size_t _i;                                                                                       \
                                                                                                             \
            RESULT = 0;                                                                                      \
            for (_i = 0; _i < (A).len; _i++)                                                                 \
                if (!FLOAT_EQUAL((double)((A_TYPE *)(A).p)[_i], (double)((B_TYPE *)(B).p)[_i])) {            \
                    RESULT = 1;                                                                              \
                    break;                                                                                   \
                }                                                                                            \
        }                                                                                                    \
    }

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
typedef enum { ONE, TWO, THREE, FOUR, FIVE, SIX } enum_key_t;

typedef enum { RED, GREEN, BLUE, WHITE, BLACK, YELLOW } enum_value_t;

enum_key_t   enum_enum_keys[NUMB_KEYS];
enum_value_t enum_enum_vals[NUMB_KEYS];
enum_value_t enum_vals_out[NUMB_KEYS];

/* Keys and values for vl-vl map */
hvl_t vl_vl_keys[NUMB_KEYS];
hvl_t vl_vl_vals[NUMB_KEYS];
hvl_t vl_vals_out[NUMB_KEYS];

/* Keys and values for vls-vls map */
char *vls_vls_keys[NUMB_KEYS];
char *vls_vls_vals[NUMB_KEYS];
char *vls_vals_out[NUMB_KEYS];

/* Keys and values for compound-compound map */
typedef struct compound_t {
    int   a;
    float b;
} compound_t;

compound_t comp_comp_keys[NUMB_KEYS];
compound_t comp_comp_vals[NUMB_KEYS];
compound_t comp_vals_out[NUMB_KEYS];

/* Keys and values for nested compound-compound map */
typedef struct nested_compound_t {
    int   a;
    float b;
    hvl_t c;
} nested_compound_t;

compound_t        nested_comp_keys[NUMB_KEYS];
nested_compound_t nested_comp_vals[NUMB_KEYS];
nested_compound_t nested_comp_out[NUMB_KEYS];

/* Keys and values for simple tconv map */
int       stconv_int_keys[NUMB_KEYS];
int       stconv_int_vals[NUMB_KEYS];
long long stconv_long_long_keys[NUMB_KEYS];
double    stconv_double_vals[NUMB_KEYS];
int       stconv_int_vals_out[NUMB_KEYS];
double    stconv_double_vals_out[NUMB_KEYS];

/* Keys and values for vl tconv map */
hvl_t vl_int_keys[NUMB_KEYS];
hvl_t vl_int_vals[NUMB_KEYS];
hvl_t vl_long_long_keys[NUMB_KEYS];
hvl_t vl_double_vals[NUMB_KEYS];
hvl_t vl_int_vals_out[NUMB_KEYS];
hvl_t vl_double_vals_out[NUMB_KEYS];

/* Datatypes for vl tconv map */
hid_t vl_int_dtype_id       = -1;
hid_t vl_long_long_dtype_id = -1;
hid_t vl_double_dtype_id    = -1;

/* Dataspace to pass to H5Treclaim() */
hid_t scalar_space_id = -1;

/*
 * Tests creating and closing a map object
 */
static int
test_create_map(hid_t file_id, const char *map_name, hid_t key_dtype, hid_t value_dtype, hid_t mcpl_id,
                hid_t mapl_id, hbool_t print_msg)
{
    hid_t   map_id      = -1;
    hid_t   id_out      = -1;
    hid_t   copied_mcpl = -1;
    htri_t  ids_equal;
    char    object_class[128];
    ssize_t ssize_ret;

    if (print_msg)
        TESTING_2("creation of map object");

    if ((map_id = H5Mcreate(file_id, map_name, key_dtype, value_dtype, H5P_DEFAULT, mcpl_id, mapl_id)) < 0) {
        H5_FAILED();
        AT();
        printf("    couldn't create map\n");
        goto error;
    } /* end if */

    /* Test H5Mget_key_type() */
    if ((id_out = H5Mget_key_type(map_id)) < 0) {
        H5_FAILED();
        AT();
        printf("    couldn't get key datatype\n");
        goto error;
    } /* end if */
    if ((ids_equal = H5Tequal(id_out, key_dtype)) < 0)
        TEST_ERROR;
    if (!ids_equal) {
        H5_FAILED();
        AT();
        printf("    retrieved key datatype does not match\n");
        goto error;
    } /* end if */
    if (H5Tclose(id_out) < 0)
        TEST_ERROR;

    /* Test H5Mget_val_type() */
    if ((id_out = H5Mget_val_type(map_id)) < 0) {
        H5_FAILED();
        AT();
        printf("    couldn't get value datatype\n");
        goto error;
    } /* end if */
    if ((ids_equal = H5Tequal(id_out, value_dtype)) < 0)
        TEST_ERROR;
    if (!ids_equal) {
        H5_FAILED();
        AT();
        printf("    retrieved value datatype does not match\n");
        goto error;
    } /* end if */
    if (H5Tclose(id_out) < 0)
        TEST_ERROR;

    /* Test H5Mget_create_plist() */
    /* Note we must copy the object class property or the lists will compare as
     * different.  This is DAOS connector specific code. */
    if ((id_out = H5Mget_create_plist(map_id)) < 0) {
        H5_FAILED();
        AT();
        printf("    couldn't get MCPL\n");
        goto error;
    } /* end if */
    if ((ssize_ret = H5daos_get_object_class(id_out, object_class, sizeof(object_class))) < 0)
        TEST_ERROR;
    if ((size_t)ssize_ret >= sizeof(object_class)) {
        H5_FAILED();
        AT();
        printf("    object class string too large (fix test)\n");
        goto error;
    } /* end if */
    if ((copied_mcpl = H5Pcopy(mcpl_id)) < 0)
        TEST_ERROR;
    if (H5daos_set_object_class(copied_mcpl, object_class) < 0)
        TEST_ERROR;
    if ((ids_equal = H5Pequal(id_out, copied_mcpl)) < 0)
        TEST_ERROR;
    if (!ids_equal) {
        H5_FAILED();
        AT();
        printf("    retrieved MCPL does not match\n");
        goto error;
    } /* end if */
    if (H5Pclose(copied_mcpl) < 0)
        TEST_ERROR;
    if (H5Pclose(id_out) < 0)
        TEST_ERROR;

    /* Test H5Mget_access_plist() */
    if ((id_out = H5Mget_access_plist(map_id)) < 0) {
        H5_FAILED();
        AT();
        printf("    couldn't get MAPL\n");
        goto error;
    } /* end if */
    if ((ids_equal = H5Pequal(id_out, mapl_id)) < 0)
        TEST_ERROR;
    if (!ids_equal) {
        H5_FAILED();
        AT();
        printf("    retrieved MAPL does not match\n");
        goto error;
    } /* end if */
    if (H5Pclose(id_out) < 0)
        TEST_ERROR;

    if (H5Mclose(map_id) < 0)
        TEST_ERROR;

    if (print_msg) {
        PASSED();
        fflush(stdout);
    }

    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Mclose(map_id);
    }
    H5E_END_TRY;

    return 1;
} /* end test_create_map() */

/*
 * Tests opening a map object
 */
static int
test_open_map(hid_t file_id, const char *map_name, hid_t key_dtype, hid_t value_dtype, hid_t mcpl_id,
              hid_t mapl_id, hbool_t print_msg)
{
    hid_t   map_id      = -1;
    hid_t   id_out      = -1;
    hid_t   copied_mcpl = -1;
    htri_t  ids_equal;
    char    object_class[128];
    ssize_t ssize_ret;

    if (print_msg)
        TESTING_2("open of map object");

    if ((map_id = H5Mopen(file_id, map_name, mapl_id)) < 0) {
        H5_FAILED();
        AT();
        printf("    couldn't open map\n");
        goto error;
    } /* end if */

    /* Test H5Mget_key_type() */
    if ((id_out = H5Mget_key_type(map_id)) < 0) {
        H5_FAILED();
        AT();
        printf("    couldn't get key datatype\n");
        goto error;
    } /* end if */
    if ((ids_equal = H5Tequal(id_out, key_dtype)) < 0)
        TEST_ERROR;
    if (!ids_equal) {
        H5_FAILED();
        AT();
        printf("    retrieved key datatype does not match\n");
        goto error;
    } /* end if */
    if (H5Tclose(id_out) < 0)
        TEST_ERROR;

    /* Test H5Mget_val_type() */
    if ((id_out = H5Mget_val_type(map_id)) < 0) {
        H5_FAILED();
        AT();
        printf("    couldn't get value datatype\n");
        goto error;
    } /* end if */
    if ((ids_equal = H5Tequal(id_out, value_dtype)) < 0)
        TEST_ERROR;
    if (!ids_equal) {
        H5_FAILED();
        AT();
        printf("    retrieved value datatype does not match\n");
        goto error;
    } /* end if */
    if (H5Tclose(id_out) < 0)
        TEST_ERROR;

    /* Test H5Mget_create_plist() */
    /* Note we must copy the object class property or the lists will compare as
     * different.  This is DAOS connector specific code. */
    if ((id_out = H5Mget_create_plist(map_id)) < 0) {
        H5_FAILED();
        AT();
        printf("    couldn't get MCPL\n");
        goto error;
    } /* end if */
    if ((ssize_ret = H5daos_get_object_class(id_out, object_class, sizeof(object_class))) < 0)
        TEST_ERROR;
    if ((size_t)ssize_ret >= sizeof(object_class)) {
        H5_FAILED();
        AT();
        printf("    object class string too large (fix test)\n");
        goto error;
    } /* end if */
    if ((copied_mcpl = H5Pcopy(mcpl_id)) < 0)
        TEST_ERROR;
    if (H5daos_set_object_class(copied_mcpl, object_class) < 0)
        TEST_ERROR;
    if ((ids_equal = H5Pequal(id_out, copied_mcpl)) < 0)
        TEST_ERROR;
    if (!ids_equal) {
        H5_FAILED();
        AT();
        printf("    retrieved MCPL does not match\n");
        goto error;
    } /* end if */
    if (H5Pclose(copied_mcpl) < 0)
        TEST_ERROR;
    if (H5Pclose(id_out) < 0)
        TEST_ERROR;

    /* Test H5Mget_access_plist() */
    if ((id_out = H5Mget_access_plist(map_id)) < 0) {
        H5_FAILED();
        AT();
        printf("    couldn't get MAPL\n");
        goto error;
    } /* end if */
    if ((ids_equal = H5Pequal(id_out, mapl_id)) < 0)
        TEST_ERROR;
    if (!ids_equal) {
        H5_FAILED();
        AT();
        printf("    retrieved MAPL does not match\n");
        goto error;
    } /* end if */
    if (H5Pclose(id_out) < 0)
        TEST_ERROR;

    if (H5Mclose(map_id) < 0)
        TEST_ERROR;

    if (print_msg) {
        PASSED();
        fflush(stdout);
    }

    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Mclose(map_id);
    }
    H5E_END_TRY;

    return 1;
} /* end test_open_map() */

/*
 * Tests setting keys in a map object
 */
static int
test_map_set(hid_t file_id, const char *map_name, hid_t key_dtype, hid_t value_dtype, hbool_t print_msg)
{
    hid_t map_id = -1;
    int   i;

    if (print_msg)
        TESTING_2("map set with keys and values");

    if ((map_id = H5Mopen(file_id, map_name, H5P_DEFAULT)) < 0)
        TEST_ERROR;

    if (!strcmp(map_name, MAP_INT_INT_NAME) ||
        !strncmp(map_name, "map_large_name_", strlen("map_large_name_"))) {
        /* Set the values */
        for (i = 0; i < NUMB_KEYS; i++)
            if (H5Mput(map_id, key_dtype, &int_int_keys[i], value_dtype, &int_int_vals[i], H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */
    }
    else if (!strcmp(map_name, MAP_ENUM_ENUM_NAME)) {
        /* Set the values */
        for (i = 0; i < NUMB_KEYS; i++)
            if (H5Mput(map_id, key_dtype, &enum_enum_keys[i], value_dtype, &enum_enum_vals[i], H5P_DEFAULT) <
                0) {
                H5_FAILED();
                AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */
    }
    else if (!strcmp(map_name, MAP_VL_VL_NAME)) {
        /* Set the values */
        for (i = 0; i < NUMB_KEYS; i++)
            if (H5Mput(map_id, key_dtype, &vl_vl_keys[i], value_dtype, &vl_vl_vals[i], H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */
    }
    else if (!strcmp(map_name, MAP_VLS_VLS_NAME)) {
        /* Set the values */
        for (i = 0; i < NUMB_KEYS; i++)
            if (H5Mput(map_id, key_dtype, &vls_vls_keys[i], value_dtype, &vls_vls_vals[i], H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */
    }
    else if (!strcmp(map_name, MAP_COMP_COMP_NAME)) {
        /* Set the values */
        for (i = 0; i < NUMB_KEYS; i++)
            if (H5Mput(map_id, key_dtype, &comp_comp_keys[i], value_dtype, &comp_comp_vals[i], H5P_DEFAULT) <
                0) {
                H5_FAILED();
                AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */
    }
    else if (!strcmp(map_name, MAP_NESTED_COMP_NAME)) {
        /* Set the values */
        for (i = 0; i < NUMB_KEYS; i++)
            if (H5Mput(map_id, key_dtype, &nested_comp_keys[i], value_dtype, &nested_comp_vals[i],
                       H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */
    }
    else if (!strcmp(map_name, MAP_SIMPLE_TCONV1_NAME)) {
        /* Set the values */
        for (i = 0; i < NUMB_KEYS; i++)
            if (H5Mput(map_id, key_dtype, &stconv_int_keys[i], value_dtype, &stconv_int_vals[i],
                       H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */
    }
    else if (!strcmp(map_name, MAP_SIMPLE_TCONV2_NAME)) {
        /* Set the values */
        for (i = 0; i < NUMB_KEYS; i++)
            if (H5Mput(map_id, key_dtype, &stconv_long_long_keys[i], value_dtype, &stconv_double_vals[i],
                       H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */
    }
    else if (!strcmp(map_name, MAP_VL_TCONV1_NAME)) {
        /* Set the values */
        for (i = 0; i < NUMB_KEYS; i++)
            if (H5Mput(map_id, key_dtype, &vl_int_keys[i], value_dtype, &vl_int_vals[i], H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */
    }
    else if (!strcmp(map_name, MAP_VL_TCONV2_NAME)) {
        /* Set the values */
        for (i = 0; i < NUMB_KEYS; i++)
            if (H5Mput(map_id, key_dtype, &vl_long_long_keys[i], value_dtype, &vl_double_vals[i],
                       H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */
    }
    else if (!strcmp(map_name, MAP_MANY_ENTRIES_NAME)) {
        /* Set the values */
        for (i = 0; i < LARGE_NUMB_KEYS; i++)
            if (H5Mput(map_id, key_dtype, &large_int_int_keys[i], value_dtype, &large_int_int_vals[i],
                       H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */
    }
    else {
        H5_FAILED();
        AT();
        printf("unknown map type\n");
        goto error;
    } /* end if */

    if (H5Mclose(map_id) < 0)
        TEST_ERROR;

    if (print_msg) {
        PASSED();
        fflush(stdout);
    }

    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Mclose(map_id);
    }
    H5E_END_TRY;

    return 1;
} /* end test_map_set() */

/*
 * Tests getting keys from a map object
 */
static int
test_map_get(hid_t file_id, const char *map_name, hid_t key_dtype, hid_t value_dtype, hbool_t print_msg)
{
    hid_t map_id = -1;
    int   i;

    if (print_msg)
        TESTING_2("map get with keys and values");

    if ((map_id = H5Mopen(file_id, map_name, H5P_DEFAULT)) < 0)
        TEST_ERROR;

    /* Get the values and check that they are correct */
    if (!strcmp(map_name, MAP_INT_INT_NAME) ||
        !strncmp(map_name, "map_large_name_", strlen("map_large_name_"))) {
        for (i = 0; i < NUMB_KEYS; i++) {
            if (H5Mget(map_id, key_dtype, &int_int_keys[i], value_dtype, &int_vals_out[i], H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if (int_vals_out[i] != int_int_vals[i]) {
                H5_FAILED();
                AT();
                printf("incorrect value returned - index is %d\n", i);
                goto error;
            } /* end if */
        }
    }
    else if (!strcmp(map_name, MAP_ENUM_ENUM_NAME)) {
        for (i = 0; i < NUMB_KEYS; i++) {
            if (H5Mget(map_id, key_dtype, &enum_enum_keys[i], value_dtype, &enum_vals_out[i], H5P_DEFAULT) <
                0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if (enum_vals_out[i] != enum_enum_vals[i]) {
                H5_FAILED();
                AT();
                printf("incorrect value returned - index is %d\n", i);
                goto error;
            } /* end if */
        }
    }
    else if (!strcmp(map_name, MAP_VL_VL_NAME)) {
        for (i = 0; i < NUMB_KEYS; i++) {
            if (H5Mget(map_id, key_dtype, &(vl_vl_keys[i]), value_dtype, &(vl_vals_out[i]), H5P_DEFAULT) <
                0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if (VLCMP(vl_vl_vals[i], vl_vals_out[i], short)) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            }

            if (H5Treclaim(value_dtype, scalar_space_id, H5P_DEFAULT, &vl_vals_out[i]) < 0) {
                H5_FAILED();
                AT();
                printf("failed to reclaim space\n");
                goto error;
            }
        } /* end for */
    }
    else if (!strcmp(map_name, MAP_VLS_VLS_NAME)) {
        for (i = 0; i < NUMB_KEYS; i++) {
            if (H5Mget(map_id, key_dtype, &(vls_vls_keys[i]), value_dtype, &(vls_vals_out[i]), H5P_DEFAULT) <
                0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if (STRCMP_NULL(vls_vls_vals[i], vls_vals_out[i])) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            }

            if (H5Treclaim(value_dtype, scalar_space_id, H5P_DEFAULT, &vls_vals_out[i]) < 0) {
                H5_FAILED();
                AT();
                printf("failed to reclaim space\n");
                goto error;
            }
        } /* end for */
    }
    else if (!strcmp(map_name, MAP_COMP_COMP_NAME)) {
        for (i = 0; i < NUMB_KEYS; i++) {
            if (H5Mget(map_id, key_dtype, &comp_comp_keys[i], value_dtype, &comp_vals_out[i], H5P_DEFAULT) <
                0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if (comp_vals_out[i].a != comp_comp_vals[i].a || comp_vals_out[i].b != comp_comp_vals[i].b) {
                H5_FAILED();
                AT();
                printf("incorrect value returned - index is %d\n", i);
                goto error;
            } /* end if */
        }
    }
    else if (!strcmp(map_name, MAP_NESTED_COMP_NAME)) {
        for (i = 0; i < NUMB_KEYS; i++) {
            if (H5Mget(map_id, key_dtype, &nested_comp_keys[i], value_dtype, &nested_comp_out[i],
                       H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if (nested_comp_out[i].a != nested_comp_vals[i].a ||
                nested_comp_out[i].b != nested_comp_vals[i].b ||
                VLCMP(nested_comp_vals[i].c, nested_comp_out[i].c, int)) {
                H5_FAILED();
                AT();
                printf("incorrect value returned - index is %d\n", i);
                goto error;
            } /* end if */

            if (H5Treclaim(value_dtype, scalar_space_id, H5P_DEFAULT, &nested_comp_out[i]) < 0) {
                H5_FAILED();
                AT();
                printf("failed to reclaim space\n");
                goto error;
            }
        }
    }
    else if (!strcmp(map_name, MAP_SIMPLE_TCONV1_NAME)) {
        long long tmp_ll;

        for (i = 0; i < NUMB_KEYS; i++) {
            if (H5Mget(map_id, H5T_NATIVE_INT, &stconv_int_keys[i], H5T_NATIVE_INT, &stconv_int_vals_out[i],
                       H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if (stconv_int_vals_out[i] != stconv_int_vals[i]) {
                H5_FAILED();
                AT();
                printf("incorrect value returned - index is %d\n", i);
                goto error;
            } /* end if */

            tmp_ll = (long long)stconv_int_keys[i];
            if (H5Mget(map_id, H5T_NATIVE_LLONG, &tmp_ll, H5T_NATIVE_DOUBLE, &stconv_double_vals_out[i],
                       H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if (!FLOAT_EQUAL(stconv_double_vals_out[i], (double)stconv_int_vals[i])) {
                H5_FAILED();
                AT();
                printf("incorrect value returned - index is %d\n", i);
                goto error;
            } /* end if */
        }
    }
    else if (!strcmp(map_name, MAP_SIMPLE_TCONV2_NAME)) {
        int tmp_int;

        for (i = 0; i < NUMB_KEYS; i++) {
            if (H5Mget(map_id, H5T_NATIVE_LLONG, &stconv_long_long_keys[i], H5T_NATIVE_DOUBLE,
                       &stconv_double_vals_out[i], H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if (!FLOAT_EQUAL(stconv_double_vals_out[i], stconv_double_vals[i])) {
                H5_FAILED();
                AT();
                printf("incorrect value returned - index is %d\n", i);
                goto error;
            } /* end if */

            tmp_int = (int)stconv_long_long_keys[i];
            if (H5Mget(map_id, H5T_NATIVE_INT, &tmp_int, H5T_NATIVE_INT, &stconv_int_vals_out[i],
                       H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if (!FLOAT_EQUAL((double)stconv_int_vals_out[i], stconv_double_vals[i])) {
                H5_FAILED();
                AT();
                printf("incorrect value returned - index is %d\n", i);
                goto error;
            } /* end if */
        }
    }
    else if (!strcmp(map_name, MAP_VL_TCONV1_NAME)) {
        int vl_cmp;

        for (i = 0; i < NUMB_KEYS; i++) {
            if (H5Mget(map_id, vl_int_dtype_id, &vl_int_keys[i], vl_int_dtype_id, &vl_int_vals_out[i],
                       H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if (VLCMP(vl_int_vals_out[i], vl_int_vals[i], int)) {
                H5_FAILED();
                AT();
                printf("incorrect value returned - index is %d\n", i);
                goto error;
            } /* end if */

            if (H5Treclaim(vl_int_dtype_id, scalar_space_id, H5P_DEFAULT, &vl_int_vals_out[i]) < 0) {
                H5_FAILED();
                AT();
                printf("failed to reclaim space\n");
                goto error;
            }

            if (H5Mget(map_id, vl_long_long_dtype_id, &vl_long_long_keys[i], vl_double_dtype_id,
                       &vl_double_vals_out[i], H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            VL_FLOAT_CMP(vl_double_vals_out[i], vl_int_vals[i], double, int, vl_cmp)
            if (vl_cmp) {
                H5_FAILED();
                AT();
                printf("incorrect value returned - index is %d\n", i);
                goto error;
            } /* end if */

            if (H5Treclaim(vl_double_dtype_id, scalar_space_id, H5P_DEFAULT, &vl_double_vals_out[i]) < 0) {
                H5_FAILED();
                AT();
                printf("failed to reclaim space\n");
                goto error;
            }
        }
    }
    else if (!strcmp(map_name, MAP_VL_TCONV2_NAME)) {
        int vl_cmp;

        for (i = 0; i < NUMB_KEYS; i++) {
            if (H5Mget(map_id, vl_long_long_dtype_id, &vl_long_long_keys[i], vl_double_dtype_id,
                       &vl_double_vals_out[i], H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            VL_FLOAT_CMP(vl_double_vals_out[i], vl_double_vals[i], double, double, vl_cmp)
            if (vl_cmp) {
                H5_FAILED();
                AT();
                printf("incorrect value returned - index is %d\n", i);
                goto error;
            } /* end if */

            if (H5Treclaim(vl_double_dtype_id, scalar_space_id, H5P_DEFAULT, &vl_double_vals_out[i]) < 0) {
                H5_FAILED();
                AT();
                printf("failed to reclaim space\n");
                goto error;
            }

            if (H5Mget(map_id, vl_int_dtype_id, &vl_int_keys[i], vl_int_dtype_id, &vl_int_vals_out[i],
                       H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            VL_FLOAT_CMP(vl_int_vals_out[i], vl_double_vals[i], int, double, vl_cmp)
            if (vl_cmp) {
                H5_FAILED();
                AT();
                printf("incorrect value returned - index is %d\n", i);
                goto error;
            } /* end if */

            if (H5Treclaim(vl_int_dtype_id, scalar_space_id, H5P_DEFAULT, &vl_int_vals_out[i]) < 0) {
                H5_FAILED();
                AT();
                printf("failed to reclaim space\n");
                goto error;
            }
        }
    }
    else if (!strcmp(map_name, MAP_MANY_ENTRIES_NAME)) {
        for (i = 0; i < LARGE_NUMB_KEYS; i++) {
            if (H5Mget(map_id, key_dtype, &large_int_int_keys[i], value_dtype, &large_int_vals_out[i],
                       H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if (large_int_vals_out[i] != large_int_int_vals[i]) {
                H5_FAILED();
                AT();
                printf("incorrect value returned - index is %d\n", i);
                goto error;
            } /* end if */
        }
    }
    else {
        H5_FAILED();
        AT();
        printf("unknown map type\n");
        goto error;
    } /* end if */

    if (H5Mclose(map_id) < 0)
        TEST_ERROR;

    if (print_msg) {
        PASSED();
        fflush(stdout);
    }

    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Mclose(map_id);
    }
    H5E_END_TRY;

    return 1;
} /* end test_map_get() */

/*
 * Tests getting a non-existent key from a map object
 */
static int
test_map_nonexistent_key(hid_t file_id, const char *map_name, hid_t key_dtype, hid_t value_dtype)
{
    hid_t  map_id = -1;
    herr_t error;
    int    i;

    TESTING_2("map with a non-existent key");

    if ((map_id = H5Mopen(file_id, map_name, H5P_DEFAULT)) < 0)
        TEST_ERROR;

    /* Get or delete a key that doesn't exist */
    if (!strcmp(map_name, MAP_INT_INT_NAME)) {
        int non_existent_key, non_existent_value, out_value;

        /* Initialize a key that doesn't exist */
        do {
            non_existent_key = rand();
            for (i = 0; i < NUMB_KEYS; i++)
                if (non_existent_key == int_int_keys[i])
                    break;
        } while (i < NUMB_KEYS);

        H5E_BEGIN_TRY
        {
            error =
                H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        /* Attempt to delete a non-existent key (should fail) */
        H5E_BEGIN_TRY
        {
            error = H5Mdelete(map_id, key_dtype, &non_existent_key, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("deleted a non-existent key!\n");
            goto error;
        } /* end if */

        /* Verify that things are still good */
        H5E_BEGIN_TRY
        {
            error =
                H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        non_existent_value = rand();

        /* Try to set the non-existent key and value to make sure it works as expected */
        if (H5Mput(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to set key-value pair\n");
            goto error;
        } /* end if */

        /* Get back the key and value just being set */
        if (H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &out_value, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to get key-value pair\n");
            goto error;
        } /* end if */

        /* Verify the value */
        if (out_value != non_existent_value) {
            H5_FAILED();
            AT();
            printf("wrong value of key-value pair\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(map_name, MAP_ENUM_ENUM_NAME)) {
        enum_key_t   non_existent_key = SIX;
        enum_value_t non_existent_value, out_value;

        H5E_BEGIN_TRY
        {
            error =
                H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        /* Attempt to delete a non-existent key (should fail) */
        H5E_BEGIN_TRY
        {
            error = H5Mdelete(map_id, key_dtype, &non_existent_key, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("deleted a non-existent key!\n");
            goto error;
        } /* end if */

        /* Verify that things are still good */
        H5E_BEGIN_TRY
        {
            error =
                H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        non_existent_value = YELLOW;

        /* Try to set the non-existent key and value to make sure it works as expected */
        if (H5Mput(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to set key-value pair\n");
            goto error;
        } /* end if */

        /* Get back the key and value just being set */
        if (H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &out_value, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to get key-value pair\n");
            goto error;
        } /* end if */

        /* Verify the value */
        if (out_value != non_existent_value) {
            H5_FAILED();
            AT();
            printf("wrong value of key-value pair\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(map_name, MAP_VL_VL_NAME)) {
        hvl_t non_existent_key, non_existent_value, out_value;

        /* Initialize non-existent key */
        non_existent_key.p   = malloc(NUMB_KEYS * sizeof(short));
        non_existent_key.len = NUMB_KEYS;
        for (i = 0; i < NUMB_KEYS; i++)
            ((short *)non_existent_key.p)[i] = (short)(i * 10 + 13);

        H5E_BEGIN_TRY
        {
            error =
                H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        /* Attempt to delete a non-existent key (should fail) */
        H5E_BEGIN_TRY
        {
            error = H5Mdelete(map_id, key_dtype, &non_existent_key, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("deleted a non-existent key!\n");
            goto error;
        } /* end if */

        /* Verify that things are still good */
        H5E_BEGIN_TRY
        {
            error =
                H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        /* Initialize non-existent value */
        non_existent_value.p   = malloc(NUMB_KEYS * sizeof(int));
        non_existent_value.len = NUMB_KEYS;
        for (i = 0; i < NUMB_KEYS; i++)
            ((int *)non_existent_value.p)[i] = rand();

        /* Try to set the non-existent key and value to make sure it works as expected */
        if (H5Mput(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to set key-value pair\n");
            goto error;
        } /* end if */

        /* Get back the key and value just being set */
        if (H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &out_value, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to get key-value pair\n");
            goto error;
        } /* end if */

        if (VLCMP(out_value, non_existent_value, int)) {
            H5_FAILED();
            AT();
            printf("wrong value of key-value pair\n");
            goto error;
        } /* end if */

        if (H5Treclaim(value_dtype, scalar_space_id, H5P_DEFAULT, &out_value) < 0) {
            H5_FAILED();
            AT();
            printf("failed to reclaim space\n");
            goto error;
        }

        free(non_existent_key.p);
        free(non_existent_value.p);
    }
    else if (!strcmp(map_name, MAP_VLS_VLS_NAME)) {
        char *non_existent_key = "deadbeef", *non_existent_value = "foobar", *out_value;

        H5E_BEGIN_TRY
        {
            error = H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &out_value, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        /* Attempt to delete a non-existent key (should fail) */
        H5E_BEGIN_TRY
        {
            error = H5Mdelete(map_id, key_dtype, &non_existent_key, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("deleted a non-existent key!\n");
            goto error;
        } /* end if */

        /* Verify that things are still good */
        H5E_BEGIN_TRY
        {
            error = H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &out_value, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        /* Try to set the non-existent key and value to make sure it works as expected */
        if (H5Mput(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to set key-value pair\n");
            goto error;
        } /* end if */

        /* Get back the key and value just being set */
        if (H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &out_value, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to get key-value pair\n");
            goto error;
        } /* end if */

        if (STRCMP_NULL(out_value, non_existent_value)) {
            H5_FAILED();
            AT();
            printf("wrong value of key-value pair\n");
            goto error;
        } /* end if */

        if (H5Treclaim(value_dtype, scalar_space_id, H5P_DEFAULT, &out_value) < 0) {
            H5_FAILED();
            AT();
            printf("failed to reclaim space\n");
            goto error;
        }
    }
    else if (!strcmp(map_name, MAP_COMP_COMP_NAME)) {
        compound_t non_existent_key, non_existent_value, out_value;

        /* Initialize non-existent key */
        non_existent_key.a = random_base - 10;
        non_existent_key.b = (float)(random_base - 100);

        H5E_BEGIN_TRY
        {
            error =
                H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        /* Attempt to delete a non-existent key (should fail) */
        H5E_BEGIN_TRY
        {
            error = H5Mdelete(map_id, key_dtype, &non_existent_key, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("deleted a non-existent key!\n");
            goto error;
        } /* end if */

        /* Verify that things are still good */
        H5E_BEGIN_TRY
        {
            error =
                H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        /* Initialize non-existent value */
        non_existent_value.a = rand();
        non_existent_value.b = (float)(rand());

        /* Try to set the non-existent key and value to make sure it works as expected */
        if (H5Mput(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to set key-value pair\n");
            goto error;
        } /* end if */

        /* Get back the key and value just being set */
        if (H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &out_value, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to get key-value pair\n");
            goto error;
        } /* end if */

        /* Verify the value */
        if (out_value.a != non_existent_value.a || out_value.b != non_existent_value.b) {
            H5_FAILED();
            AT();
            printf("wrong value of key-value pair\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(map_name, MAP_NESTED_COMP_NAME)) {
        compound_t        non_existent_key;
        nested_compound_t non_existent_value, out_value;

        /* Initialize non-existent key */
        non_existent_key.a = random_base - 10;
        non_existent_key.b = (float)(random_base - 100);

        H5E_BEGIN_TRY
        {
            error =
                H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        /* Attempt to delete a non-existent key (should fail) */
        H5E_BEGIN_TRY
        {
            error = H5Mdelete(map_id, key_dtype, &non_existent_key, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("deleted a non-existent key!\n");
            goto error;
        } /* end if */

        /* Verify that things are still good */
        H5E_BEGIN_TRY
        {
            error =
                H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        /* Initialize non-existent value */
        non_existent_value.a     = rand();
        non_existent_value.b     = (float)(rand());
        non_existent_value.c.p   = malloc(NUMB_KEYS * sizeof(int));
        non_existent_value.c.len = NUMB_KEYS;
        for (i = 0; i < NUMB_KEYS; i++)
            ((int *)non_existent_value.c.p)[i] = rand();

        /* Try to set the non-existent key and value to make sure it works as expected */
        if (H5Mput(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to set key-value pair\n");
            goto error;
        } /* end if */

        /* Get back the key and value just being set */
        if (H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &out_value, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to get key-value pair\n");
            goto error;
        } /* end if */

        /* Verify the value */
        if (out_value.a != non_existent_value.a || out_value.b != non_existent_value.b ||
            VLCMP(out_value.c, non_existent_value.c, int)) {
            H5_FAILED();
            AT();
            printf("wrong value of key-value pair\n");
            goto error;
        } /* end if */

        if (H5Treclaim(value_dtype, scalar_space_id, H5P_DEFAULT, &out_value) < 0) {
            H5_FAILED();
            AT();
            printf("failed to reclaim space\n");
            goto error;
        }

        free(non_existent_value.c.p);
    }
    else if (!strcmp(map_name, MAP_SIMPLE_TCONV1_NAME)) {
        int non_existent_key, non_existent_value, out_value;

        /* Initialize a key that doesn't exist */
        do {
            non_existent_key = rand();
            for (i = 0; i < NUMB_KEYS; i++)
                if (non_existent_key == stconv_int_keys[i])
                    break;
        } while (i < NUMB_KEYS);

        H5E_BEGIN_TRY
        {
            error =
                H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        /* Attempt to delete a non-existent key (should fail) */
        H5E_BEGIN_TRY
        {
            error = H5Mdelete(map_id, key_dtype, &non_existent_key, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("deleted a non-existent key!\n");
            goto error;
        } /* end if */

        /* Verify that things are still good */
        H5E_BEGIN_TRY
        {
            error =
                H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        non_existent_value = rand();

        /* Try to set the non-existent key and value to make sure it works as expected */
        if (H5Mput(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to set key-value pair\n");
            goto error;
        } /* end if */

        /* Get back the key and value just being set */
        if (H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &out_value, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to get key-value pair\n");
            goto error;
        } /* end if */

        /* Verify the value */
        if (out_value != non_existent_value) {
            H5_FAILED();
            AT();
            printf("wrong value of key-value pair\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(map_name, MAP_SIMPLE_TCONV2_NAME)) {
        long long non_existent_key;
        double    non_existent_value, out_value;

        /* Initialize a key that doesn't exist */
        do {
            non_existent_key = rand();
            for (i = 0; i < NUMB_KEYS; i++)
                if (non_existent_key == stconv_long_long_keys[i])
                    break;
        } while (i < NUMB_KEYS);

        H5E_BEGIN_TRY
        {
            error =
                H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        /* Attempt to delete a non-existent key (should fail) */
        H5E_BEGIN_TRY
        {
            error = H5Mdelete(map_id, key_dtype, &non_existent_key, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("deleted a non-existent key!\n");
            goto error;
        } /* end if */

        /* Verify that things are still good */
        H5E_BEGIN_TRY
        {
            error =
                H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        non_existent_value = rand();

        /* Try to set the non-existent key and value to make sure it works as expected */
        if (H5Mput(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to set key-value pair\n");
            goto error;
        } /* end if */

        /* Get back the key and value just being set */
        if (H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &out_value, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to get key-value pair\n");
            goto error;
        } /* end if */

        /* Verify the value */
        if (!FLOAT_EQUAL(out_value, non_existent_value)) {
            H5_FAILED();
            AT();
            printf("wrong value of key-value pair\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(map_name, MAP_VL_TCONV1_NAME)) {
        hvl_t non_existent_key, non_existent_value, out_value;
        int   nonexist_key_buf[10];
        int   nonexist_val_buf[10];

        /* Initialize a key that doesn't exist */
        non_existent_key.len = (size_t)rand() % 10 + 1;
        do {
            nonexist_key_buf[0] = rand();
            for (i = 0; i < NUMB_KEYS; i++)
                if (nonexist_key_buf[0] == ((int *)vl_int_keys[i].p)[0])
                    break;
        } while (i < NUMB_KEYS);
        for (i = 1; i < (int)non_existent_key.len; i++)
            nonexist_key_buf[i] = rand();
        non_existent_key.p = nonexist_key_buf;

        H5E_BEGIN_TRY
        {
            error =
                H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        /* Attempt to delete a non-existent key (should fail) */
        H5E_BEGIN_TRY
        {
            error = H5Mdelete(map_id, key_dtype, &non_existent_key, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("deleted a non-existent key!\n");
            goto error;
        } /* end if */

        /* Verify that things are still good */
        H5E_BEGIN_TRY
        {
            error =
                H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        non_existent_value.len = (size_t)rand() % 10 + 1;
        for (i = 0; i < (int)non_existent_value.len; i++)
            nonexist_val_buf[i] = rand();
        non_existent_value.p = nonexist_val_buf;

        /* Try to set the non-existent key and value to make sure it works as expected */
        if (H5Mput(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to set key-value pair\n");
            goto error;
        } /* end if */

        /* Get back the key and value just being set */
        if (H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &out_value, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to get key-value pair\n");
            goto error;
        } /* end if */

        /* Verify the value */
        if (VLCMP(out_value, non_existent_value, int)) {
            H5_FAILED();
            AT();
            printf("wrong value of key-value pair\n");
            goto error;
        } /* end if */

        if (H5Treclaim(value_dtype, scalar_space_id, H5P_DEFAULT, &out_value) < 0) {
            H5_FAILED();
            AT();
            printf("failed to reclaim space\n");
            goto error;
        }
    }
    else if (!strcmp(map_name, MAP_VL_TCONV2_NAME)) {
        hvl_t     non_existent_key, non_existent_value, out_value;
        long long nonexist_key_buf[10];
        double    nonexist_val_buf[10];
        int       vl_cmp;

        /* Initialize a key that doesn't exist */
        non_existent_key.len = (size_t)rand() % 10 + 1;
        do {
            nonexist_key_buf[0] = (long long)rand();
            for (i = 0; i < NUMB_KEYS; i++)
                if (nonexist_key_buf[0] == ((long long *)vl_long_long_keys[i].p)[0])
                    break;
        } while (i < NUMB_KEYS);
        for (i = 1; i < (int)non_existent_key.len; i++)
            nonexist_key_buf[i] = (long long)rand();
        non_existent_key.p = nonexist_key_buf;

        H5E_BEGIN_TRY
        {
            error =
                H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        /* Attempt to delete a non-existent key (should fail) */
        H5E_BEGIN_TRY
        {
            error = H5Mdelete(map_id, key_dtype, &non_existent_key, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("deleted a non-existent key!\n");
            goto error;
        } /* end if */

        /* Verify that things are still good */
        H5E_BEGIN_TRY
        {
            error =
                H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT);
        }
        H5E_END_TRY;

        if (error >= 0) {
            H5_FAILED();
            AT();
            printf("succeeded to get non-existent key\n");
            goto error;
        } /* end if */

        non_existent_value.len = (size_t)rand() % 10 + 1;
        for (i = 0; i < (int)non_existent_value.len; i++)
            nonexist_val_buf[i] = (double)rand();
        non_existent_value.p = nonexist_val_buf;

        /* Try to set the non-existent key and value to make sure it works as expected */
        if (H5Mput(map_id, key_dtype, &non_existent_key, value_dtype, &non_existent_value, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to set key-value pair\n");
            goto error;
        } /* end if */

        /* Get back the key and value just being set */
        if (H5Mget(map_id, key_dtype, &non_existent_key, value_dtype, &out_value, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to get key-value pair\n");
            goto error;
        } /* end if */

        /* Verify the value */
        VL_FLOAT_CMP(out_value, non_existent_value, double, double, vl_cmp)
        if (vl_cmp) {
            H5_FAILED();
            AT();
            printf("wrong value of key-value pair\n");
            goto error;
        } /* end if */

        if (H5Treclaim(value_dtype, scalar_space_id, H5P_DEFAULT, &out_value) < 0) {
            H5_FAILED();
            AT();
            printf("failed to reclaim space\n");
            goto error;
        }
    }
    else {
        H5_FAILED();
        AT();
        printf("unknown map type\n");
        goto error;
    } /* end if */

    if (H5Mclose(map_id) < 0)
        TEST_ERROR;

    PASSED();
    fflush(stdout);

    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Mclose(map_id);
    }
    H5E_END_TRY;

    return 1;
} /* end test_map_nonexistent_key() */

/*
 * Tests updating keys from a map object
 */
static int
test_map_update(hid_t file_id, const char *map_name, hid_t key_dtype, hid_t value_dtype)
{
    hid_t map_id = -1;
    int   i;

    TESTING_2("map update with keys and values");

    if ((map_id = H5Mopen(file_id, map_name, H5P_DEFAULT)) < 0)
        TEST_ERROR;

    for (i = 0; i < NUMB_KEYS; i++) {
        enum_enum_keys[i] = i;
        enum_enum_vals[i] = i + 1;
    } /* end for */

    /* Update the values and check that they are correct */
    if (!strcmp(map_name, MAP_INT_INT_NAME)) {
        int updated_values[NUMB_KEYS];

        for (i = 0; i < NUMB_KEYS; i++) {
            updated_values[i] = rand();

            if (H5Mput(map_id, key_dtype, &int_int_keys[i], value_dtype, &updated_values[i], H5P_DEFAULT) <
                0) {
                H5_FAILED();
                AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */

            if (H5Mget(map_id, key_dtype, &int_int_keys[i], value_dtype, &int_vals_out[i], H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if (int_vals_out[i] != updated_values[i]) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */
        }
    }
    else if (!strcmp(map_name, MAP_ENUM_ENUM_NAME)) {
        enum_value_t updated_values[NUMB_KEYS];

        for (i = 0; i < NUMB_KEYS; i++) {
            updated_values[i] = NUMB_KEYS + 1 - i;

            if (H5Mput(map_id, key_dtype, &enum_enum_keys[i], value_dtype, &updated_values[i], H5P_DEFAULT) <
                0) {
                H5_FAILED();
                AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */

            if (H5Mget(map_id, key_dtype, &enum_enum_keys[i], value_dtype, &enum_vals_out[i], H5P_DEFAULT) <
                0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if (enum_vals_out[i] != updated_values[i]) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */
        }
    }
    else if (!strcmp(map_name, MAP_VL_VL_NAME)) {
        hvl_t updated_values[NUMB_KEYS];
        int   j;

        for (i = 0; i < NUMB_KEYS; i++) {
            updated_values[i].p   = malloc((size_t)(i + 2 * NUMB_KEYS) * sizeof(int));
            updated_values[i].len = (size_t)(i + 2 * NUMB_KEYS);

            for (j = 0; j < (i + 2 * NUMB_KEYS); j++)
                ((int *)updated_values[i].p)[j] = rand();

            if (H5Mput(map_id, key_dtype, &vl_vl_keys[i], value_dtype, &updated_values[i], H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */

            if (H5Mget(map_id, key_dtype, &vl_vl_keys[i], value_dtype, &vl_vals_out[i], H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if (VLCMP(updated_values[i], vl_vals_out[i], int)) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            }

            if (H5Treclaim(value_dtype, scalar_space_id, H5P_DEFAULT, &vl_vals_out[i]) < 0) {
                H5_FAILED();
                AT();
                printf("failed to reclaim space\n");
                goto error;
            }

            free(updated_values[i].p);
            updated_values[i].p = NULL;
        }
    }
    else if (!strcmp(map_name, MAP_VLS_VLS_NAME)) {
        char *updated_values[NUMB_KEYS] = {"", NULL, "cat", "parasaurolophus"};

        for (i = 0; i < NUMB_KEYS; i++) {
            if (H5Mput(map_id, key_dtype, &vls_vls_keys[i], value_dtype, &updated_values[i], H5P_DEFAULT) <
                0) {
                H5_FAILED();
                AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */

            if (H5Mget(map_id, key_dtype, &vls_vls_keys[i], value_dtype, &vls_vals_out[i], H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if (STRCMP_NULL(updated_values[i], vls_vals_out[i])) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            }

            if (H5Treclaim(value_dtype, scalar_space_id, H5P_DEFAULT, &vls_vals_out[i]) < 0) {
                H5_FAILED();
                AT();
                printf("failed to reclaim space\n");
                goto error;
            }
        }
    }
    else if (!strcmp(map_name, MAP_COMP_COMP_NAME)) {
        compound_t updated_values[NUMB_KEYS];

        for (i = 0; i < NUMB_KEYS; i++) {
            updated_values[i].a = rand();
            updated_values[i].b = (float)rand();

            if (H5Mput(map_id, key_dtype, &comp_comp_keys[i], value_dtype, &updated_values[i], H5P_DEFAULT) <
                0) {
                H5_FAILED();
                AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */

            if (H5Mget(map_id, key_dtype, &comp_comp_keys[i], value_dtype, &comp_vals_out[i], H5P_DEFAULT) <
                0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if (comp_vals_out[i].a != updated_values[i].a || comp_vals_out[i].b != updated_values[i].b) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */
        }
    }
    else if (!strcmp(map_name, MAP_NESTED_COMP_NAME)) {
        nested_compound_t updated_values[NUMB_KEYS];
        int               j;

        for (i = 0; i < NUMB_KEYS; i++) {
            updated_values[i].a     = rand();
            updated_values[i].b     = (float)rand();
            updated_values[i].c.p   = malloc((size_t)(i + 2 * NUMB_KEYS) * sizeof(int));
            updated_values[i].c.len = (size_t)(i + 2 * NUMB_KEYS);

            for (j = 0; j < (i + 2 * NUMB_KEYS); j++)
                ((int *)updated_values[i].c.p)[j] = rand();

            if (H5Mput(map_id, key_dtype, &nested_comp_keys[i], value_dtype, &updated_values[i],
                       H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */

            if (H5Mget(map_id, key_dtype, &nested_comp_keys[i], value_dtype, &nested_comp_out[i],
                       H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if (nested_comp_out[i].a != updated_values[i].a || nested_comp_out[i].b != updated_values[i].b ||
                VLCMP(nested_comp_out[i].c, updated_values[i].c, int)) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */

            if (H5Treclaim(value_dtype, scalar_space_id, H5P_DEFAULT, &nested_comp_out[i]) < 0) {
                H5_FAILED();
                AT();
                printf("failed to reclaim space\n");
                goto error;
            }

            free(updated_values[i].c.p);
            updated_values[i].c.p = NULL;
        }
    }
    else if (!strcmp(map_name, MAP_SIMPLE_TCONV1_NAME)) {
        int updated_values[NUMB_KEYS];

        for (i = 0; i < NUMB_KEYS; i++) {
            updated_values[i] = rand();

            if (H5Mput(map_id, key_dtype, &stconv_int_keys[i], value_dtype, &updated_values[i], H5P_DEFAULT) <
                0) {
                H5_FAILED();
                AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */

            if (H5Mget(map_id, key_dtype, &stconv_int_keys[i], value_dtype, &stconv_int_vals_out[i],
                       H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if (stconv_int_vals_out[i] != updated_values[i]) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */
        }
    }
    else if (!strcmp(map_name, MAP_SIMPLE_TCONV2_NAME)) {
        double updated_values[NUMB_KEYS];

        for (i = 0; i < NUMB_KEYS; i++) {
            updated_values[i] = (double)rand();

            if (H5Mput(map_id, key_dtype, &stconv_long_long_keys[i], value_dtype, &updated_values[i],
                       H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */

            if (H5Mget(map_id, key_dtype, &stconv_long_long_keys[i], value_dtype, &stconv_double_vals_out[i],
                       H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if (!FLOAT_EQUAL(stconv_double_vals_out[i], updated_values[i])) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */
        }
    }
    else if (!strcmp(map_name, MAP_VL_TCONV1_NAME)) {
        hvl_t  updated_value;
        int    updated_value_buf[10];
        size_t j;

        updated_value.p = updated_value_buf;

        for (i = 0; i < NUMB_KEYS; i++) {
            updated_value.len = (size_t)rand() % 10 + 1;
            for (j = 0; j < updated_value.len; j++)
                updated_value_buf[j] = rand();

            if (H5Mput(map_id, key_dtype, &vl_int_keys[i], value_dtype, &updated_value, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */

            if (H5Mget(map_id, key_dtype, &vl_int_keys[i], value_dtype, &vl_int_vals_out[i], H5P_DEFAULT) <
                0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            if (VLCMP(vl_int_vals_out[i], updated_value, int)) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */

            if (H5Treclaim(value_dtype, scalar_space_id, H5P_DEFAULT, &vl_int_vals_out[i]) < 0) {
                H5_FAILED();
                AT();
                printf("failed to reclaim space\n");
                goto error;
            }
        }
    }
    else if (!strcmp(map_name, MAP_VL_TCONV2_NAME)) {
        hvl_t  updated_value;
        double updated_value_buf[10];
        size_t j;
        int    vl_cmp;

        updated_value.p = updated_value_buf;

        for (i = 0; i < NUMB_KEYS; i++) {
            updated_value.len = (size_t)rand() % 10 + 1;
            for (j = 0; j < updated_value.len; j++)
                updated_value_buf[j] = (double)rand();

            if (H5Mput(map_id, key_dtype, &vl_long_long_keys[i], value_dtype, &updated_value, H5P_DEFAULT) <
                0) {
                H5_FAILED();
                AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */

            if (H5Mget(map_id, key_dtype, &vl_long_long_keys[i], value_dtype, &vl_double_vals_out[i],
                       H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to get key-value pair\n");
                goto error;
            } /* end if */

            VL_FLOAT_CMP(vl_double_vals_out[i], updated_value, double, double, vl_cmp)
            if (vl_cmp) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */

            if (H5Treclaim(value_dtype, scalar_space_id, H5P_DEFAULT, &vl_double_vals_out[i]) < 0) {
                H5_FAILED();
                AT();
                printf("failed to reclaim space\n");
                goto error;
            }
        }
    }
    else {
        H5_FAILED();
        AT();
        printf("unknown map type\n");
        goto error;
    } /* end if */

    if (H5Mclose(map_id) < 0)
        TEST_ERROR;

    PASSED();
    fflush(stdout);

    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Mclose(map_id);
    }
    H5E_END_TRY;

    return 1;
} /* end test_map_update() */

/*
 * Tests checking if keys exist in a map object
 */
static int
test_map_exists(hid_t file_id, const char *map_name, hid_t key_dtype)
{
    hid_t   map_id = -1;
    hbool_t exists;
    int     i;

    TESTING_2("map exists with the keys and values");

    if ((map_id = H5Mopen(file_id, map_name, H5P_DEFAULT)) < 0)
        TEST_ERROR;

    if (!strcmp(map_name, MAP_INT_INT_NAME)) {
        int nonexist_key_int;

        for (i = 0; i < NUMB_KEYS; i++) {
            /* Check if the existing keys exist (should all be TRUE) */
            if (H5Mexists(map_id, key_dtype, &int_int_keys[i], &exists, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if (!exists) {
                H5_FAILED();
                AT();
                printf("incorrect value returned: index %d\n", i);
                goto error;
            } /* end if */
        }     /* end for */

        /* Initialize a key that doesn't exist */
        do {
            nonexist_key_int = rand();
            for (i = 0; i < NUMB_KEYS; i++)
                if (nonexist_key_int == int_int_keys[i])
                    break;
        } while (i < NUMB_KEYS);

        /* Check if the nonexisting key exists (should be FALSE) */
        if (H5Mexists(map_id, key_dtype, &nonexist_key_int, &exists, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if (exists) {
            H5_FAILED();
            AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(map_name, MAP_ENUM_ENUM_NAME)) {
        enum_key_t nonexist_key_enum;

        for (i = 0; i < NUMB_KEYS; i++) {
            if (H5Mexists(map_id, key_dtype, &enum_enum_keys[i], &exists, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if (!exists) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */
        }     /* end for */

        nonexist_key_enum = NUMB_KEYS + 1;

        /* Check if the nonexisting key exists (should be FALSE) */
        if (H5Mexists(map_id, key_dtype, &nonexist_key_enum, &exists, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if (exists) {
            H5_FAILED();
            AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(map_name, MAP_VL_VL_NAME)) {
        hvl_t nonexist_key_vl;

        for (i = 0; i < NUMB_KEYS; i++) {
            if (H5Mexists(map_id, key_dtype, &(vl_vl_keys[i]), &exists, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if (!exists) {
                H5_FAILED();
                AT();
                printf("incorrect value returned: %d\n", i);
                goto error;
            } /* end if */
        }     /* end for */

        /* Initialize non-existent key */
        nonexist_key_vl.p   = malloc(10 * sizeof(short));
        nonexist_key_vl.len = 10;
        for (i = 0; i < 10; i++)
            ((short *)nonexist_key_vl.p)[i] = (short)(100 + i);

        /* Check if the nonexisting key exists (should be FALSE) */
        if (H5Mexists(map_id, key_dtype, &nonexist_key_vl, &exists, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if (exists) {
            H5_FAILED();
            AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */

        free(nonexist_key_vl.p);
    }
    else if (!strcmp(map_name, MAP_VLS_VLS_NAME)) {
        char *nonexist_key_vls = "Dodgson here";

        for (i = 0; i < NUMB_KEYS; i++) {
            if (H5Mexists(map_id, key_dtype, &(vls_vls_keys[i]), &exists, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if (!exists) {
                H5_FAILED();
                AT();
                printf("incorrect value returned: %d\n", i);
                goto error;
            } /* end if */
        }     /* end for */

        /* Check if the nonexisting key exists (should be FALSE) */
        if (H5Mexists(map_id, key_dtype, &nonexist_key_vls, &exists, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if (exists) {
            H5_FAILED();
            AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(map_name, MAP_COMP_COMP_NAME)) {
        compound_t nonexist_key_comp;

        for (i = 0; i < NUMB_KEYS; i++) {
            if (H5Mexists(map_id, key_dtype, &comp_comp_keys[i], &exists, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if (!exists) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */
        }     /* end for */

        nonexist_key_comp.a = random_base - 1;
        nonexist_key_comp.b = (float)(random_base - 1);

        /* Check if the nonexisting key exists (should be FALSE) */
        if (H5Mexists(map_id, key_dtype, &nonexist_key_comp, &exists, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if (exists) {
            H5_FAILED();
            AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(map_name, MAP_NESTED_COMP_NAME)) {
        compound_t nonexist_key_nested;

        for (i = 0; i < NUMB_KEYS; i++) {
            if (H5Mexists(map_id, key_dtype, &nested_comp_keys[i], &exists, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if (!exists) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */
        }     /* end for */

        nonexist_key_nested.a = random_base - 1;
        nonexist_key_nested.b = (float)(random_base - 1);

        /* Check if the nonexisting key exists (should be FALSE) */
        if (H5Mexists(map_id, key_dtype, &nonexist_key_nested, &exists, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if (exists) {
            H5_FAILED();
            AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(map_name, MAP_SIMPLE_TCONV1_NAME)) {
        int nonexist_key;

        for (i = 0; i < NUMB_KEYS; i++) {
            /* Check if the existing keys exist (should all be TRUE) */
            if (H5Mexists(map_id, key_dtype, &stconv_int_keys[i], &exists, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if (!exists) {
                H5_FAILED();
                AT();
                printf("incorrect value returned: index %d\n", i);
                goto error;
            } /* end if */
        }     /* end for */

        /* Initialize a key that doesn't exist */
        do {
            nonexist_key = rand();
            for (i = 0; i < NUMB_KEYS; i++)
                if (nonexist_key == stconv_int_keys[i])
                    break;
        } while (i < NUMB_KEYS);

        /* Check if the nonexisting key exists (should be FALSE) */
        if (H5Mexists(map_id, key_dtype, &nonexist_key, &exists, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if (exists) {
            H5_FAILED();
            AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(map_name, MAP_SIMPLE_TCONV2_NAME)) {
        long long nonexist_key;

        for (i = 0; i < NUMB_KEYS; i++) {
            /* Check if the existing keys exist (should all be TRUE) */
            if (H5Mexists(map_id, key_dtype, &stconv_long_long_keys[i], &exists, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if (!exists) {
                H5_FAILED();
                AT();
                printf("incorrect value returned: index %d\n", i);
                goto error;
            } /* end if */
        }     /* end for */

        /* Initialize a key that doesn't exist */
        do {
            nonexist_key = (long long)rand();
            for (i = 0; i < NUMB_KEYS; i++)
                if (nonexist_key == stconv_int_keys[i])
                    break;
        } while (i < NUMB_KEYS);

        /* Check if the nonexisting key exists (should be FALSE) */
        if (H5Mexists(map_id, key_dtype, &nonexist_key, &exists, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if (exists) {
            H5_FAILED();
            AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(map_name, MAP_VL_TCONV1_NAME)) {
        hvl_t nonexist_key;
        int   nonexist_key_buf[10];

        for (i = 0; i < NUMB_KEYS; i++) {
            /* Check if the existing keys exist (should all be TRUE) */
            if (H5Mexists(map_id, key_dtype, &vl_int_keys[i], &exists, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if (!exists) {
                H5_FAILED();
                AT();
                printf("incorrect value returned: index %d\n", i);
                goto error;
            } /* end if */
        }     /* end for */

        /* Initialize a key that doesn't exist */
        nonexist_key.len = (size_t)rand() % 10 + 1;
        do {
            nonexist_key_buf[0] = rand();
            for (i = 0; i < NUMB_KEYS; i++)
                if (nonexist_key_buf[0] == ((int *)vl_int_keys[i].p)[0])
                    break;
        } while (i < NUMB_KEYS);
        for (i = 1; i < (int)nonexist_key.len; i++)
            nonexist_key_buf[i] = rand();
        nonexist_key.p = nonexist_key_buf;

        /* Check if the nonexisting key exists (should be FALSE) */
        if (H5Mexists(map_id, key_dtype, &nonexist_key, &exists, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if (exists) {
            H5_FAILED();
            AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(map_name, MAP_VL_TCONV2_NAME)) {
        hvl_t     nonexist_key;
        long long nonexist_key_buf[10];

        for (i = 0; i < NUMB_KEYS; i++) {
            /* Check if the existing keys exist (should all be TRUE) */
            if (H5Mexists(map_id, key_dtype, &vl_long_long_keys[i], &exists, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if (!exists) {
                H5_FAILED();
                AT();
                printf("incorrect value returned: index %d\n", i);
                goto error;
            } /* end if */
        }     /* end for */

        /* Initialize a key that doesn't exist */
        nonexist_key.len = (size_t)rand() % 10 + 1;
        do {
            nonexist_key_buf[0] = rand();
            for (i = 0; i < NUMB_KEYS; i++)
                if (nonexist_key_buf[0] == ((int *)vl_long_long_keys[i].p)[0])
                    break;
        } while (i < NUMB_KEYS);
        for (i = 1; i < (int)nonexist_key.len; i++)
            nonexist_key_buf[i] = (long long)rand();
        nonexist_key.p = nonexist_key_buf;

        /* Check if the nonexisting key exists (should be FALSE) */
        if (H5Mexists(map_id, key_dtype, &nonexist_key, &exists, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if (exists) {
            H5_FAILED();
            AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(map_name, MAP_MANY_ENTRIES_NAME)) {
        for (i = 0; i < LARGE_NUMB_KEYS; i++) {
            /* Check if the existing keys exist (should all be TRUE) */
            if (H5Mexists(map_id, key_dtype, &large_int_int_keys[i], &exists, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if (!exists) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */
        }     /* end for */
    }
    else {
        H5_FAILED();
        AT();
        printf("unknown map type\n");
        goto error;
    } /* end if */

    if (H5Mclose(map_id) < 0)
        TEST_ERROR;

    PASSED();
    fflush(stdout);

    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Mclose(map_id);
    }
    H5E_END_TRY;

    return 1;
} /* end test_map_exists() */

/*
 * Tests iterating over all keys in a map object
 */
typedef struct {
    int  *keys_visited;
    int   ncalls;
    int   stop_at;
    char *map_name;
} iterate_ud_t;

static herr_t
map_iterate_cb(hid_t map_id, const void *_key, void *_iterate_ud)
{
    iterate_ud_t *iterate_ud = (iterate_ud_t *)_iterate_ud;
    int           i;

    (void)map_id; /* silence compiler */

    /* Check parameters */
    if (!_key) {
        H5_FAILED();
        AT();
        printf("key is NULL\n");
        goto error;
    } /* end if */
    if (!iterate_ud) {
        H5_FAILED();
        AT();
        printf("op_data is NULL\n");
        goto error;
    } /* end if */
    if (!iterate_ud->map_name) {
        H5_FAILED();
        AT();
        printf("op_data is NULL\n");
        goto error;
    } /* end if */

    /* Mark key visited */
    if (!strcmp(iterate_ud->map_name, MAP_INT_INT_NAME)) {
        for (i = 0; i < NUMB_KEYS; i++) {
            if (int_int_keys[i] == *((const int *)_key)) {
                iterate_ud->keys_visited[i]++;
                break;
            } /* end if */
        }

        if (i == NUMB_KEYS) {
            H5_FAILED();
            AT();
            printf("returned key not found in local key array: %d among ", *((const int *)_key));
            for (i = 0; i < NUMB_KEYS; i++)
                printf("%d, ", int_int_keys[i]);
            printf("\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(iterate_ud->map_name, MAP_ENUM_ENUM_NAME)) {
        for (i = 0; i < NUMB_KEYS; i++) {
            if (enum_enum_keys[i] == *((const enum_key_t *)_key)) {
                iterate_ud->keys_visited[i]++;
                break;
            } /* end if */
        }

        if (i == NUMB_KEYS) {
            H5_FAILED();
            AT();
            printf("returned key not found in local key array\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(iterate_ud->map_name, MAP_VL_VL_NAME)) {
        for (i = 0; i < NUMB_KEYS; i++) {
            if (!VLCMP(vl_vl_keys[i], *((const hvl_t *)_key), short)) {
                iterate_ud->keys_visited[i]++;
                break;
            } /* end if */
        }

        if (i == NUMB_KEYS) {
            H5_FAILED();
            AT();
            printf("returned key not found in local key array\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(iterate_ud->map_name, MAP_VLS_VLS_NAME)) {
        for (i = 0; i < NUMB_KEYS; i++) {
            if (!STRCMP_NULL(vls_vls_keys[i], *((char *const *)_key))) {
                iterate_ud->keys_visited[i]++;
                break;
            } /* end if */
        }

        if (i == NUMB_KEYS) {
            H5_FAILED();
            AT();
            printf("returned key not found in local key array\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(iterate_ud->map_name, MAP_COMP_COMP_NAME)) {
        for (i = 0; i < NUMB_KEYS; i++) {
            if (comp_comp_keys[i].a == ((const compound_t *)_key)->a &&
                comp_comp_keys[i].b == ((const compound_t *)_key)->b) {
                iterate_ud->keys_visited[i]++;
                break;
            } /* end if */
        }

        if (i == NUMB_KEYS) {
            H5_FAILED();
            AT();
            printf("returned key not found in local key array\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(iterate_ud->map_name, MAP_NESTED_COMP_NAME)) {
        for (i = 0; i < NUMB_KEYS; i++) {
            if (nested_comp_keys[i].a == ((const compound_t *)_key)->a &&
                nested_comp_keys[i].b == ((const compound_t *)_key)->b) {
                iterate_ud->keys_visited[i]++;
                break;
            } /* end if */
        }

        if (i == NUMB_KEYS) {
            H5_FAILED();
            AT();
            printf("key not found\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(iterate_ud->map_name, MAP_SIMPLE_TCONV1_NAME)) {
        for (i = 0; i < NUMB_KEYS; i++) {
            if (stconv_int_keys[i] == *((const int *)_key)) {
                iterate_ud->keys_visited[i]++;
                break;
            } /* end if */
        }

        if (i == NUMB_KEYS) {
            H5_FAILED();
            AT();
            printf("returned key not found in local key array: %d among ", *((const int *)_key));
            for (i = 0; i < NUMB_KEYS; i++)
                printf("%d, ", stconv_int_keys[i]);
            printf("\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(iterate_ud->map_name, MAP_SIMPLE_TCONV2_NAME)) {
        for (i = 0; i < NUMB_KEYS; i++) {
            if (stconv_long_long_keys[i] == *((const long long *)_key)) {
                iterate_ud->keys_visited[i]++;
                break;
            } /* end if */
        }

        if (i == NUMB_KEYS) {
            H5_FAILED();
            AT();
            printf("returned key not found in local key array: %lld among ", *((const long long *)_key));
            for (i = 0; i < NUMB_KEYS; i++)
                printf("%lld, ", stconv_long_long_keys[i]);
            printf("\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(iterate_ud->map_name, MAP_VL_TCONV1_NAME)) {
        for (i = 0; i < NUMB_KEYS; i++) {
            if (!VLCMP(vl_int_keys[i], *((const hvl_t *)_key), int)) {
                iterate_ud->keys_visited[i]++;
                break;
            } /* end if */
        }

        if (i == NUMB_KEYS) {
            H5_FAILED();
            AT();
            printf("returned key not found in local key array\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(iterate_ud->map_name, MAP_VL_TCONV2_NAME)) {
        for (i = 0; i < NUMB_KEYS; i++) {
            if (!VLCMP(vl_long_long_keys[i], *((const hvl_t *)_key), long long)) {
                iterate_ud->keys_visited[i]++;
                break;
            } /* end if */
        }

        if (i == NUMB_KEYS) {
            H5_FAILED();
            AT();
            printf("returned key not found in local key array\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(iterate_ud->map_name, MAP_MANY_ENTRIES_NAME)) {
        /* Work backwards from algorithm used to generate the keys to find the
         * index into large_int_int_keys */
        i = *((const int *)_key) % LARGE_NUMB_KEYS;

        if (large_int_int_keys[i] != *((const int *)_key)) {
            H5_FAILED();
            AT();
            printf("returned key not found at expected location in local key array\n");
            goto error;
        } /* end if */

        iterate_ud->keys_visited[i]++;
    }
    else {
        H5_FAILED();
        AT();
        printf("unknown map type\n");
        goto error;
    } /* end if */

    /* Check for short circuit */
    if (++iterate_ud->ncalls == iterate_ud->stop_at)
        return 1;

    return 0;

error:
    return -1;
} /* end map_iterate_cb */

static int
test_map_iterate(hid_t file_id, const char *map_name, hid_t key_dtype)
{
    hid_t        map_id = -1;
    iterate_ud_t iterate_ud;
    hsize_t      idx;
    int          nkeys;
    int          ret;
    int          i;

    TESTING_2("iterating over keys in map");

    if ((map_id = H5Mopen(file_id, map_name, H5P_DEFAULT)) < 0)
        TEST_ERROR;

    /* Reset iterate_cb */
    memset(&iterate_ud, 0, sizeof(iterate_ud));

    /* Copy the map name to the struct */
    iterate_ud.map_name = strdup(map_name);

    if (!strcmp(map_name, MAP_MANY_ENTRIES_NAME))
        iterate_ud.keys_visited = calloc(LARGE_NUMB_KEYS, sizeof(int));
    else
        iterate_ud.keys_visited = calloc(NUMB_KEYS, sizeof(int));

    /* Iterate over all keys */
    iterate_ud.stop_at = -1;
    idx                = 0;
    if ((ret = H5Miterate(map_id, &idx, key_dtype, map_iterate_cb, &iterate_ud, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        AT();
        printf("failed to iterate over map\n");
        goto error;
    } /* end if */
    if (ret != 0) {
        H5_FAILED();
        AT();
        printf("incorrect return code from H5Miterate\n");
        goto error;
    } /* end if */

    /* Check that all keys were visited exactly once */
    if (!strcmp(map_name, MAP_MANY_ENTRIES_NAME)) {
        if (idx != (hsize_t)LARGE_NUMB_KEYS) {
            H5_FAILED();
            AT();
            printf("incorrect value of idx after H5Miterate\n");
            goto error;
        } /* end if */

        for (i = 0; i < LARGE_NUMB_KEYS; i++)
            if (iterate_ud.keys_visited[i] != 1) {
                H5_FAILED();
                AT();
                printf("key visited an incorrect number of times\n");
                goto error;
            } /* end if */
    }
    else {
        if (idx != (hsize_t)NUMB_KEYS) {
            H5_FAILED();
            AT();
            printf("incorrect value of idx after H5Miterate\n");
            goto error;
        } /* end if */

        for (i = 0; i < NUMB_KEYS; i++)
            if (iterate_ud.keys_visited[i] != 1) {
                H5_FAILED();
                AT();
                printf("key visited an incorrect number of times\n");
                goto error;
            } /* end if */
    }

    /* Reset iterate_cb */
    if (iterate_ud.map_name) {
        free(iterate_ud.map_name);
        iterate_ud.map_name = NULL;
    }

    if (iterate_ud.keys_visited) {
        free(iterate_ud.keys_visited);
        iterate_ud.keys_visited = NULL;
    }

    memset(&iterate_ud, 0, sizeof(iterate_ud));

    /* Copy the map name to the struct */
    iterate_ud.map_name = strdup(map_name);

    if (!strcmp(map_name, MAP_MANY_ENTRIES_NAME))
        iterate_ud.keys_visited = calloc(LARGE_NUMB_KEYS, sizeof(int));
    else
        iterate_ud.keys_visited = calloc(NUMB_KEYS, sizeof(int));

    /* Iterate but stop after the second key */
    iterate_ud.stop_at = 2;
    idx                = 0;
    if ((ret = H5Miterate(map_id, &idx, key_dtype, map_iterate_cb, &iterate_ud, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        AT();
        printf("failed to iterate over map\n");
        goto error;
    } /* end if */
    if (ret != 1) {
        H5_FAILED();
        AT();
        printf("incorrect return code from H5Miterate\n");
        goto error;
    } /* end if */
    if (idx != (hsize_t)2) {
        H5_FAILED();
        AT();
        printf("incorrect value of idx after H5Miterate\n");
        goto error;
    } /* end if */

    /* Check that all keys were visited zero or one times, and that exactly two
     * keys were visited in total. */
    nkeys = 0;
    if (!strcmp(map_name, MAP_MANY_ENTRIES_NAME)) {
        for (i = 0; i < LARGE_NUMB_KEYS; i++) {
            if (iterate_ud.keys_visited[i] > 1) {
                H5_FAILED();
                AT();
                printf("key visited an incorrect number of times\n");
                goto error;
            } /* end if */
            nkeys += iterate_ud.keys_visited[i];
        }
    }
    else {
        for (i = 0; i < NUMB_KEYS; i++) {
            if (iterate_ud.keys_visited[i] > 1) {
                H5_FAILED();
                AT();
                printf("key visited an incorrect number of times\n");
                goto error;
            } /* end if */
            nkeys += iterate_ud.keys_visited[i];
        }
    }

    if (nkeys != 2) {
        H5_FAILED();
        AT();
        printf("incorrect number of keys visited\n");
        goto error;
    } /* end if */

    if (iterate_ud.map_name) {
        free(iterate_ud.map_name);
        iterate_ud.map_name = NULL;
    }

    if (iterate_ud.keys_visited) {
        free(iterate_ud.keys_visited);
        iterate_ud.keys_visited = NULL;
    }
    if (H5Mclose(map_id) < 0)
        TEST_ERROR;

    PASSED();
    fflush(stdout);

    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Mclose(map_id);
    }
    H5E_END_TRY;

    if (iterate_ud.map_name)
        free(iterate_ud.map_name);

    if (iterate_ud.keys_visited)
        free(iterate_ud.keys_visited);

    return 1;
} /* end test_map_iterate() */

/*
 * Iterating over all keys to verify that the deleted key is not in the map
 */
typedef struct {
    char *map_name;
} iterate_ud2_t;

static herr_t
map_iterate_cb2(hid_t map_id, const void *_key, void *_iterate_ud)
{
    iterate_ud2_t *iterate_ud = (iterate_ud2_t *)_iterate_ud;

    (void)map_id; /* silence compiler */

    /* Check parameters */
    if (!_key) {
        H5_FAILED();
        AT();
        printf("key is NULL\n");
        goto error;
    } /* end if */
    if (!iterate_ud) {
        H5_FAILED();
        AT();
        printf("op_data is NULL\n");
        goto error;
    } /* end if */
    if (!iterate_ud->map_name) {
        H5_FAILED();
        AT();
        printf("op_data is NULL\n");
        goto error;
    } /* end if */

    /* Mark key visited */
    if (!strcmp(iterate_ud->map_name, MAP_INT_INT_NAME)) {
        if (int_int_keys[0] == *((const int *)_key)) {
            H5_FAILED();
            AT();
            printf("deleted key still exists: %d\n", *((const int *)_key));
            goto error;
        } /* end if */
    }
    else if (!strcmp(iterate_ud->map_name, MAP_ENUM_ENUM_NAME)) {
        if (enum_enum_keys[0] == *((const enum_key_t *)_key)) {
            H5_FAILED();
            AT();
            printf("deleted key still exists: %d\n", *((const enum_key_t *)_key));
            goto error;
        } /* end if */
    }
    else if (!strcmp(iterate_ud->map_name, MAP_VL_VL_NAME)) {
        if (!VLCMP(vl_vl_keys[0], *((const hvl_t *)_key), short)) {
            H5_FAILED();
            AT();
            printf("deleted key still exists\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(iterate_ud->map_name, MAP_VLS_VLS_NAME)) {
        if (!STRCMP_NULL(vls_vls_keys[0], *((char *const *)_key))) {
            H5_FAILED();
            AT();
            printf("deleted key still exists\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(iterate_ud->map_name, MAP_COMP_COMP_NAME)) {
        if (comp_comp_keys[0].a == ((const compound_t *)_key)->a &&
            comp_comp_keys[0].b == ((const compound_t *)_key)->b) {
            H5_FAILED();
            AT();
            printf("deleted key still exists\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(iterate_ud->map_name, MAP_NESTED_COMP_NAME)) {
        if (nested_comp_keys[0].a == ((const compound_t *)_key)->a &&
            nested_comp_keys[0].b == ((const compound_t *)_key)->b) {
            H5_FAILED();
            AT();
            printf("deleted key still exists\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(iterate_ud->map_name, MAP_SIMPLE_TCONV1_NAME)) {
        if (stconv_int_keys[0] == *((const int *)_key)) {
            H5_FAILED();
            AT();
            printf("deleted key still exists: %d\n", *((const int *)_key));
            goto error;
        } /* end if */
    }
    else if (!strcmp(iterate_ud->map_name, MAP_SIMPLE_TCONV2_NAME)) {
        if (stconv_long_long_keys[0] == *((const long long *)_key)) {
            H5_FAILED();
            AT();
            printf("deleted key still exists: %lld\n", *((const long long *)_key));
            goto error;
        } /* end if */
    }
    else if (!strcmp(iterate_ud->map_name, MAP_VL_TCONV1_NAME)) {
        if (!VLCMP(vl_int_keys[0], *((const hvl_t *)_key), int)) {
            H5_FAILED();
            AT();
            printf("deleted key still exists\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(iterate_ud->map_name, MAP_VL_TCONV2_NAME)) {
        if (!VLCMP(vl_long_long_keys[0], *((const hvl_t *)_key), long long)) {
            H5_FAILED();
            AT();
            printf("deleted key still exists\n");
            goto error;
        } /* end if */
    }
    else if (!strcmp(iterate_ud->map_name, MAP_MANY_ENTRIES_NAME)) {
        if (large_int_int_keys[0] == *((const int *)_key)) {
            H5_FAILED();
            AT();
            printf("deleted key still exists\n");
            goto error;
        } /* end if */
    }
    else {
        H5_FAILED();
        AT();
        printf("unknown map type\n");
        goto error;
    } /* end if */

    return 0;

error:
    return -1;
} /* end map_iterate_cb2 */

/*
 * Tests checking if an entry can be removed in a map object
 */
static int
test_map_delete_key(hid_t file_id, const char *map_name, hid_t key_dtype)
{
    hid_t         map_id = -1;
    hbool_t       exists;
    iterate_ud2_t iterate_ud;
    hsize_t       idx;
    int           ret;
    int           i;

    TESTING_2("removing an entry by the key");

    /* Reset iterate_ud */
    memset(&iterate_ud, 0, sizeof(iterate_ud));

    if ((map_id = H5Mopen(file_id, map_name, H5P_DEFAULT)) < 0)
        TEST_ERROR;

    /* Check if the existing keys exist (should all be TRUE) */
    if (!strcmp(map_name, MAP_INT_INT_NAME)) {
        /* Delete the first entry */
        if (H5Mdelete(map_id, key_dtype, &int_int_keys[0], H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to remove an entry by the key\n");
            goto error;
        } /* end if */

        /* Check if the deleted key still exists (should be FALSE) */
        if (H5Mexists(map_id, key_dtype, &int_int_keys[0], &exists, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if (exists) {
            H5_FAILED();
            AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */

        /* Check the rest of entries still exist */
        for (i = 1; i < NUMB_KEYS; i++) {
            if (H5Mexists(map_id, key_dtype, &int_int_keys[i], &exists, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if (!exists) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */
        }     /* end for */
    }
    else if (!strcmp(map_name, MAP_ENUM_ENUM_NAME)) {
        /* Delete the first entry */
        if (H5Mdelete(map_id, key_dtype, &enum_enum_keys[0], H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to remove an entry by the key\n");
            goto error;
        } /* end if */

        /* Check if the deleted key still exists (should be FALSE) */
        if (H5Mexists(map_id, key_dtype, &enum_enum_keys[0], &exists, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if (exists) {
            H5_FAILED();
            AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */

        /* Check the rest of entries still exist */
        for (i = 1; i < NUMB_KEYS; i++) {
            if (H5Mexists(map_id, key_dtype, &enum_enum_keys[i], &exists, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if (!exists) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */
        }     /* end for */
    }
    else if (!strcmp(map_name, MAP_VL_VL_NAME)) {
        /* Delete the first entry */
        if (H5Mdelete(map_id, key_dtype, &(vl_vl_keys[0]), H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to remove an entry by the key\n");
            goto error;
        } /* end if */

        /* Check if the nonexisting key exists (should be FALSE) */
        if (H5Mexists(map_id, key_dtype, &(vl_vl_keys[0]), &exists, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if (exists) {
            H5_FAILED();
            AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */

        /* Check the rest of entries still exist */
        for (i = 1; i < NUMB_KEYS; i++) {
            if (H5Mexists(map_id, key_dtype, &(vl_vl_keys[i]), &exists, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if (!exists) {
                H5_FAILED();
                AT();
                printf("incorrect value returned: %d\n", i);
                goto error;
            } /* end if */
        }     /* end for */
    }
    else if (!strcmp(map_name, MAP_VLS_VLS_NAME)) {
        /* Delete the first entry */
        if (H5Mdelete(map_id, key_dtype, &(vls_vls_keys[0]), H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to remove an entry by the key\n");
            goto error;
        } /* end if */

        /* Check if the nonexisting key exists (should be FALSE) */
        if (H5Mexists(map_id, key_dtype, &(vls_vls_keys[0]), &exists, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if (exists) {
            H5_FAILED();
            AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */

        /* Check the rest of entries still exist */
        for (i = 1; i < NUMB_KEYS; i++) {
            if (H5Mexists(map_id, key_dtype, &(vls_vls_keys[i]), &exists, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if (!exists) {
                H5_FAILED();
                AT();
                printf("incorrect value returned: %d\n", i);
                goto error;
            } /* end if */
        }     /* end for */
    }
    else if (!strcmp(map_name, MAP_COMP_COMP_NAME)) {
        /* Delete the first entry */
        if (H5Mdelete(map_id, key_dtype, &comp_comp_keys[0], H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to remove an entry by the key\n");
            goto error;
        } /* end if */

        /* Check if the deleted key still exists (should be FALSE) */
        if (H5Mexists(map_id, key_dtype, &comp_comp_keys[0], &exists, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if (exists) {
            H5_FAILED();
            AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */

        /* Check the rest of entries still exist */
        for (i = 1; i < NUMB_KEYS; i++) {
            if (H5Mexists(map_id, key_dtype, &comp_comp_keys[i], &exists, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if (!exists) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */
        }     /* end for */
    }
    else if (!strcmp(map_name, MAP_NESTED_COMP_NAME)) {
        /* Delete the first entry */
        if (H5Mdelete(map_id, key_dtype, &nested_comp_keys[0], H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to remove an entry by the key\n");
            goto error;
        } /* end if */

        /* Check if the deleted key still exists (should be FALSE) */
        if (H5Mexists(map_id, key_dtype, &nested_comp_keys[0], &exists, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if (exists) {
            H5_FAILED();
            AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */

        /* Check the rest of entries still exist */
        for (i = 1; i < NUMB_KEYS; i++) {
            if (H5Mexists(map_id, key_dtype, &nested_comp_keys[i], &exists, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if (!exists) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */
        }     /* end for */
    }
    else if (!strcmp(map_name, MAP_SIMPLE_TCONV1_NAME)) {
        /* Delete the first entry */
        if (H5Mdelete(map_id, key_dtype, &stconv_int_keys[0], H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to remove an entry by the key\n");
            goto error;
        } /* end if */

        /* Check if the deleted key still exists (should be FALSE) */
        if (H5Mexists(map_id, key_dtype, &stconv_int_keys[0], &exists, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if (exists) {
            H5_FAILED();
            AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */

        /* Check the rest of entries still exist */
        for (i = 1; i < NUMB_KEYS; i++) {
            if (H5Mexists(map_id, key_dtype, &stconv_int_keys[i], &exists, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if (!exists) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */
        }     /* end for */
    }
    else if (!strcmp(map_name, MAP_SIMPLE_TCONV2_NAME)) {
        /* Delete the first entry */
        if (H5Mdelete(map_id, key_dtype, &stconv_long_long_keys[0], H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to remove an entry by the key\n");
            goto error;
        } /* end if */

        /* Check if the deleted key still exists (should be FALSE) */
        if (H5Mexists(map_id, key_dtype, &stconv_long_long_keys[0], &exists, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if (exists) {
            H5_FAILED();
            AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */

        /* Check the rest of entries still exist */
        for (i = 1; i < NUMB_KEYS; i++) {
            if (H5Mexists(map_id, key_dtype, &stconv_long_long_keys[i], &exists, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if (!exists) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */
        }     /* end for */
    }
    else if (!strcmp(map_name, MAP_VL_TCONV1_NAME)) {
        /* Delete the first entry */
        if (H5Mdelete(map_id, key_dtype, &vl_int_keys[0], H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to remove an entry by the key\n");
            goto error;
        } /* end if */

        /* Check if the deleted key still exists (should be FALSE) */
        if (H5Mexists(map_id, key_dtype, &vl_int_keys[0], &exists, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if (exists) {
            H5_FAILED();
            AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */

        /* Check the rest of entries still exist */
        for (i = 1; i < NUMB_KEYS; i++) {
            if (H5Mexists(map_id, key_dtype, &vl_int_keys[i], &exists, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if (!exists) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */
        }     /* end for */
    }
    else if (!strcmp(map_name, MAP_VL_TCONV2_NAME)) {
        /* Delete the first entry */
        if (H5Mdelete(map_id, key_dtype, &vl_long_long_keys[0], H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to remove an entry by the key\n");
            goto error;
        } /* end if */

        /* Check if the deleted key still exists (should be FALSE) */
        if (H5Mexists(map_id, key_dtype, &vl_long_long_keys[0], &exists, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if (exists) {
            H5_FAILED();
            AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */

        /* Check the rest of entries still exist */
        for (i = 1; i < NUMB_KEYS; i++) {
            if (H5Mexists(map_id, key_dtype, &vl_long_long_keys[i], &exists, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if (!exists) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */
        }     /* end for */
    }
    else if (!strcmp(map_name, MAP_MANY_ENTRIES_NAME)) {
        /* Delete the first entry */
        if (H5Mdelete(map_id, key_dtype, &large_int_int_keys[0], H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to remove an entry by the key\n");
            goto error;
        } /* end if */

        /* Check if the deleted key still exists (should be FALSE) */
        if (H5Mexists(map_id, key_dtype, &large_int_int_keys[0], &exists, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to check if key exists\n");
            goto error;
        } /* end if */

        if (exists) {
            H5_FAILED();
            AT();
            printf("incorrect value returned\n");
            goto error;
        } /* end if */

        /* Check the rest of entries still exist */
        for (i = 1; i < LARGE_NUMB_KEYS; i++) {
            if (H5Mexists(map_id, key_dtype, &large_int_int_keys[i], &exists, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check if key exists\n");
                goto error;
            } /* end if */

            if (!exists) {
                H5_FAILED();
                AT();
                printf("incorrect value returned\n");
                goto error;
            } /* end if */
        }     /* end for */
    }
    else {
        H5_FAILED();
        AT();
        printf("unknown map type\n");
        goto error;
    } /* end if */

    /* Copy the map name to the struct */
    iterate_ud.map_name = strdup(map_name);

    /* Iterate over all keys to make sure the deleted one no longer exists */
    idx = 0;
    if ((ret = H5Miterate(map_id, &idx, key_dtype, map_iterate_cb2, &iterate_ud, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        AT();
        printf("failed to iterate over map\n");
        goto error;
    } /* end if */
    if (ret != 0) {
        H5_FAILED();
        AT();
        printf("incorrect return code from H5Miterate\n");
        goto error;
    } /* end if */

    /* In test_map_nonexistent_key(), a non-existent entry is added.  So the number of entries should be
     * still be NUMB_KEYS.  But for the case of large number of entries, test_map_nonexistent_key() is
     * skipped. */
    if (!strcmp(map_name, MAP_MANY_ENTRIES_NAME)) {
        if (idx != (hsize_t)LARGE_NUMB_KEYS - 1) {
            H5_FAILED();
            AT();
            printf("incorrect value of idx after H5Miterate: %llu\n", (long long unsigned)idx);
            goto error;
        } /* end if */
    }
    else {
        if (idx != (hsize_t)NUMB_KEYS) {
            H5_FAILED();
            AT();
            printf("incorrect value of idx after H5Miterate: %llu\n", (long long unsigned)idx);
            goto error;
        } /* end if */
    }

    /* Reset iterate_cb */
    if (iterate_ud.map_name) {
        free(iterate_ud.map_name);
        iterate_ud.map_name = NULL;
    }

    if (H5Mclose(map_id) < 0)
        TEST_ERROR;

    PASSED();
    fflush(stdout);

    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Mclose(map_id);
    }
    H5E_END_TRY;

    if (iterate_ud.map_name)
        free(iterate_ud.map_name);

    return 1;
} /* end test_map_delete_key() */

/*
 * Tests H5Mget_count()
 */
static int
test_map_get_count(hid_t file_id, const char *map_name, hsize_t expected_count, hbool_t print_msg)
{
    hid_t   map_id = -1;
    hsize_t count;

    if (print_msg)
        TESTING_2("H5Mget_count()");

    if ((map_id = H5Mopen(file_id, map_name, H5P_DEFAULT)) < 0)
        TEST_ERROR;

    if (H5Mget_count(map_id, &count, H5P_DEFAULT) < 0) {
        H5_FAILED();
        AT();
        printf("failed to count map keys\n");
        goto error;
    } /* end if */
    if (count != expected_count) {
        H5_FAILED();
        AT();
        printf("incorrect value returned by H5Mget_count: %llu expected: %llu\n", (long long unsigned)count,
               (long long unsigned)expected_count);
        goto error;
    } /* end if */

    if (H5Mclose(map_id) < 0)
        TEST_ERROR;

    if (print_msg) {
        PASSED();
        fflush(stdout);
    }

    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Mclose(map_id);
    }
    H5E_END_TRY;

    return 1;
} /* end test_map_get_count() */

static int
test_integer(hid_t file_id)
{
    hid_t mcpl_id = -1, mapl_id = -1;
    int   i, j;
    int   nerrors = 0;

    TESTING("integer as the datatype of keys and values");
    HDputs("");

    /* Generate random keys and values */
    for (i = 0; i < NUMB_KEYS; i++) {
        do {
            int_int_keys[i] = rand();
            for (j = 0; j < i; j++)
                if (int_int_keys[i] == int_int_keys[j])
                    break;
        } while (j < i);
        int_int_vals[i] = rand();
    } /* end for */

    /* Create property lists */
    if ((mcpl_id = H5Pcreate(H5P_MAP_CREATE)) < 0)
        goto error;
    if ((mapl_id = H5Pcreate(H5P_MAP_ACCESS)) < 0)
        goto error;

    nerrors +=
        test_create_map(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT, mcpl_id, mapl_id, TRUE);
    nerrors +=
        test_open_map(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT, mcpl_id, mapl_id, TRUE);
    nerrors += test_map_set(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT, TRUE);
    nerrors += test_map_get_count(file_id, MAP_INT_INT_NAME, NUMB_KEYS, TRUE);
    nerrors += test_map_get(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT, TRUE);
    nerrors += test_map_exists(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT);
    nerrors += test_map_iterate(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT);
    nerrors += test_map_update(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT);
    nerrors += test_map_nonexistent_key(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT);
    nerrors += test_map_get_count(file_id, MAP_INT_INT_NAME, NUMB_KEYS + 1, TRUE);
    nerrors += test_map_delete_key(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT);
    nerrors += test_map_get_count(file_id, MAP_INT_INT_NAME, NUMB_KEYS, TRUE);

    if (H5Pclose(mcpl_id) < 0)
        goto error;
    if (H5Pclose(mapl_id) < 0)
        goto error;

    return nerrors;

error:
    return ++nerrors;
}

static int
test_enum(hid_t file_id)
{
    hid_t        mcpl_id = -1, mapl_id = -1;
    hid_t        key_dtype_id = -1, value_dtype_id = -1;
    enum_value_t val;
    enum_key_t   key;
    int          nerrors = 0;
    int          i;

    TESTING("enum as the datatype of keys and values");
    HDputs("");

    if ((value_dtype_id = H5Tcreate(H5T_ENUM, sizeof(enum_value_t))) < 0)
        goto error;
    if (H5Tenum_insert(value_dtype_id, "RED", CPTR(val, RED)) < 0)
        goto error;
    if (H5Tenum_insert(value_dtype_id, "GREEN", CPTR(val, GREEN)) < 0)
        goto error;
    if (H5Tenum_insert(value_dtype_id, "BLUE", CPTR(val, BLUE)) < 0)
        goto error;
    if (H5Tenum_insert(value_dtype_id, "WHITE", CPTR(val, WHITE)) < 0)
        goto error;
    if (H5Tenum_insert(value_dtype_id, "BLACK", CPTR(val, BLACK)) < 0)
        goto error;
    if (H5Tcommit2(file_id, "value_datatype", value_dtype_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) < 0)
        goto error;

    if ((key_dtype_id = H5Tcreate(H5T_ENUM, sizeof(enum_value_t))) < 0)
        goto error;
    if (H5Tenum_insert(key_dtype_id, "ONE", CPTR(key, ONE)) < 0)
        goto error;
    if (H5Tenum_insert(key_dtype_id, "TWO", CPTR(key, TWO)) < 0)
        goto error;
    if (H5Tenum_insert(key_dtype_id, "THREE", CPTR(key, THREE)) < 0)
        goto error;
    if (H5Tenum_insert(key_dtype_id, "FOUR", CPTR(key, FOUR)) < 0)
        goto error;
    if (H5Tenum_insert(key_dtype_id, "FIVE", CPTR(key, FIVE)) < 0)
        goto error;
    if (H5Tcommit2(file_id, "key_datatype", key_dtype_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) < 0)
        goto error;

    /* Generate enum keys and values */
    for (i = 0; i < NUMB_KEYS; i++) {
        enum_enum_keys[i] = i;
        enum_enum_vals[i] = i + 1;
    } /* end for */

    /* Create property lists */
    if ((mcpl_id = H5Pcreate(H5P_MAP_CREATE)) < 0)
        goto error;
    if ((mapl_id = H5Pcreate(H5P_MAP_ACCESS)) < 0)
        goto error;

    nerrors +=
        test_create_map(file_id, MAP_ENUM_ENUM_NAME, key_dtype_id, value_dtype_id, mcpl_id, mapl_id, TRUE);
    nerrors +=
        test_open_map(file_id, MAP_ENUM_ENUM_NAME, key_dtype_id, value_dtype_id, mcpl_id, mapl_id, TRUE);
    nerrors += test_map_set(file_id, MAP_ENUM_ENUM_NAME, key_dtype_id, value_dtype_id, TRUE);
    nerrors += test_map_get_count(file_id, MAP_ENUM_ENUM_NAME, NUMB_KEYS, TRUE);
    nerrors += test_map_get(file_id, MAP_ENUM_ENUM_NAME, key_dtype_id, value_dtype_id, TRUE);
    nerrors += test_map_exists(file_id, MAP_ENUM_ENUM_NAME, key_dtype_id);
    nerrors += test_map_iterate(file_id, MAP_ENUM_ENUM_NAME, key_dtype_id);
    nerrors += test_map_update(file_id, MAP_ENUM_ENUM_NAME, key_dtype_id, value_dtype_id);
    nerrors += test_map_nonexistent_key(file_id, MAP_ENUM_ENUM_NAME, key_dtype_id, value_dtype_id);
    nerrors += test_map_get_count(file_id, MAP_ENUM_ENUM_NAME, NUMB_KEYS + 1, TRUE);
    nerrors += test_map_delete_key(file_id, MAP_ENUM_ENUM_NAME, key_dtype_id);
    nerrors += test_map_get_count(file_id, MAP_ENUM_ENUM_NAME, NUMB_KEYS, TRUE);

    if (H5Tclose(key_dtype_id) < 0)
        goto error;
    if (H5Tclose(value_dtype_id) < 0)
        goto error;

    if (H5Pclose(mcpl_id) < 0)
        goto error;
    if (H5Pclose(mapl_id) < 0)
        goto error;

    return nerrors;

error:
    return ++nerrors;
}

static int
test_vl(hid_t file_id)
{
    hid_t mcpl_id = -1, mapl_id = -1;
    hid_t key_dtype_id = -1, value_dtype_id = -1;
    int   nerrors = 0;
    int   i, j;

    TESTING("variable-length as the datatype of keys and values");
    HDputs("");

    key_dtype_id   = H5Tvlen_create(H5T_NATIVE_SHORT);
    value_dtype_id = H5Tvlen_create(H5T_NATIVE_INT);

    /* Allocate and initialize VL data to keys and values */
    for (i = 0; i < NUMB_KEYS; i++) {
        vl_vl_keys[i].p   = malloc((size_t)(i + 1) * sizeof(short));
        vl_vl_keys[i].len = (size_t)(i + 1);
        for (j = 0; j < (i + 1); j++)
            ((short *)(vl_vl_keys[i].p))[j] = (short)(i * 10 + j + 7);

        vl_vl_vals[i].p   = malloc((size_t)(i + NUMB_KEYS) * sizeof(int));
        vl_vl_vals[i].len = (size_t)(i + NUMB_KEYS);
        for (j = 0; j < (i + NUMB_KEYS); j++)
            ((int *)vl_vl_vals[i].p)[j] = random_base + j;
    } /* end for */

    /* Create property lists */
    if ((mcpl_id = H5Pcreate(H5P_MAP_CREATE)) < 0)
        goto error;
    if ((mapl_id = H5Pcreate(H5P_MAP_ACCESS)) < 0)
        goto error;

    nerrors += test_create_map(file_id, MAP_VL_VL_NAME, key_dtype_id, value_dtype_id, mcpl_id, mapl_id, TRUE);
    nerrors += test_open_map(file_id, MAP_VL_VL_NAME, key_dtype_id, value_dtype_id, mcpl_id, mapl_id, TRUE);
    nerrors += test_map_set(file_id, MAP_VL_VL_NAME, key_dtype_id, value_dtype_id, TRUE);
    nerrors += test_map_get_count(file_id, MAP_VL_VL_NAME, NUMB_KEYS, TRUE);
    nerrors += test_map_get(file_id, MAP_VL_VL_NAME, key_dtype_id, value_dtype_id, TRUE);
    nerrors += test_map_exists(file_id, MAP_VL_VL_NAME, key_dtype_id);
    nerrors += test_map_iterate(file_id, MAP_VL_VL_NAME, key_dtype_id);
    nerrors += test_map_update(file_id, MAP_VL_VL_NAME, key_dtype_id, value_dtype_id);
    nerrors += test_map_nonexistent_key(file_id, MAP_VL_VL_NAME, key_dtype_id, value_dtype_id);
    nerrors += test_map_get_count(file_id, MAP_VL_VL_NAME, NUMB_KEYS + 1, TRUE);
    nerrors += test_map_delete_key(file_id, MAP_VL_VL_NAME, key_dtype_id);
    nerrors += test_map_get_count(file_id, MAP_VL_VL_NAME, NUMB_KEYS, TRUE);

    if (H5Tclose(key_dtype_id) < 0)
        goto error;
    if (H5Tclose(value_dtype_id) < 0)
        goto error;

    if (H5Pclose(mcpl_id) < 0)
        goto error;
    if (H5Pclose(mapl_id) < 0)
        goto error;

    /* Free VL data */
    for (i = 0; i < NUMB_KEYS; i++) {
        free(vl_vl_keys[i].p);
        free(vl_vl_vals[i].p);
    } /* end for */

    return nerrors;

error:
    return ++nerrors;
}

static int
test_vls(hid_t file_id)
{
    hid_t mcpl_id = -1, mapl_id = -1;
    hid_t dtype_id = -1;
    int   nerrors  = 0;

    TESTING("variable-length strings as the datatype of keys and values");
    HDputs("");

    if ((dtype_id = H5Tcreate(H5T_STRING, H5T_VARIABLE)) < 0) {
        H5_FAILED();
        AT();
        printf("failed to create datatype\n");
        goto error;
    } /* end if */

    /* Initialize VLS data to keys and values */
    assert(NUMB_KEYS == 4);
    vls_vls_keys[0] =
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore "
        "et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut "
        "aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse "
        "cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in "
        "culpa qui officia deserunt mollit anim id est laborum.";
    vls_vls_keys[1] = NULL;
    vls_vls_keys[2] = "";
    vls_vls_keys[3] = "?";
    vls_vls_vals[0] = NULL;
    vls_vls_vals[1] = "!";
    vls_vls_vals[2] = "The quick brown fox jumps over the lazy dog";
    vls_vls_vals[3] = "";

    /* Create property lists */
    if ((mcpl_id = H5Pcreate(H5P_MAP_CREATE)) < 0)
        goto error;
    if ((mapl_id = H5Pcreate(H5P_MAP_ACCESS)) < 0)
        goto error;

    nerrors += test_create_map(file_id, MAP_VLS_VLS_NAME, dtype_id, dtype_id, mcpl_id, mapl_id, TRUE);
    nerrors += test_open_map(file_id, MAP_VLS_VLS_NAME, dtype_id, dtype_id, mcpl_id, mapl_id, TRUE);
    nerrors += test_map_set(file_id, MAP_VLS_VLS_NAME, dtype_id, dtype_id, TRUE);
    nerrors += test_map_get_count(file_id, MAP_VLS_VLS_NAME, NUMB_KEYS, TRUE);
    nerrors += test_map_get(file_id, MAP_VLS_VLS_NAME, dtype_id, dtype_id, TRUE);
    nerrors += test_map_exists(file_id, MAP_VLS_VLS_NAME, dtype_id);
    nerrors += test_map_iterate(file_id, MAP_VLS_VLS_NAME, dtype_id);
    nerrors += test_map_update(file_id, MAP_VLS_VLS_NAME, dtype_id, dtype_id);
    nerrors += test_map_nonexistent_key(file_id, MAP_VLS_VLS_NAME, dtype_id, dtype_id);
    nerrors += test_map_get_count(file_id, MAP_VLS_VLS_NAME, NUMB_KEYS + 1, TRUE);
    nerrors += test_map_delete_key(file_id, MAP_VLS_VLS_NAME, dtype_id);
    nerrors += test_map_get_count(file_id, MAP_VLS_VLS_NAME, NUMB_KEYS, TRUE);

    if (H5Tclose(dtype_id) < 0)
        goto error;

    if (H5Pclose(mcpl_id) < 0)
        goto error;
    if (H5Pclose(mapl_id) < 0)
        goto error;

    return nerrors;

error:
    return ++nerrors;
}

static int
test_compound(hid_t file_id)
{
    hid_t mcpl_id = -1, mapl_id = -1;
    hid_t dtype_id = -1;
    int   i;
    int   nerrors = 0;

    TESTING("compound as the datatype of keys and values");
    HDputs("");

    dtype_id = H5Tcreate(H5T_COMPOUND, sizeof(compound_t));
    H5Tinsert(dtype_id, "a_name", HOFFSET(compound_t, a), H5T_NATIVE_INT);
    H5Tinsert(dtype_id, "b_name", HOFFSET(compound_t, b), H5T_NATIVE_FLOAT);

    /* Generate random keys and values */
    for (i = 0; i < NUMB_KEYS; i++) {
        comp_comp_keys[i].a = random_base + i;
        comp_comp_keys[i].b = (float)(random_base + i * i);
        comp_comp_vals[i].a = rand();
        comp_comp_vals[i].b = (float)rand();
    } /* end for */

    /* Create property lists */
    if ((mcpl_id = H5Pcreate(H5P_MAP_CREATE)) < 0)
        goto error;
    if ((mapl_id = H5Pcreate(H5P_MAP_ACCESS)) < 0)
        goto error;

    nerrors += test_create_map(file_id, MAP_COMP_COMP_NAME, dtype_id, dtype_id, mcpl_id, mapl_id, TRUE);
    nerrors += test_open_map(file_id, MAP_COMP_COMP_NAME, dtype_id, dtype_id, mcpl_id, mapl_id, TRUE);
    nerrors += test_map_set(file_id, MAP_COMP_COMP_NAME, dtype_id, dtype_id, TRUE);
    nerrors += test_map_get_count(file_id, MAP_COMP_COMP_NAME, NUMB_KEYS, TRUE);
    nerrors += test_map_get(file_id, MAP_COMP_COMP_NAME, dtype_id, dtype_id, TRUE);
    nerrors += test_map_exists(file_id, MAP_COMP_COMP_NAME, dtype_id);
    nerrors += test_map_iterate(file_id, MAP_COMP_COMP_NAME, dtype_id);
    nerrors += test_map_update(file_id, MAP_COMP_COMP_NAME, dtype_id, dtype_id);
    nerrors += test_map_nonexistent_key(file_id, MAP_COMP_COMP_NAME, dtype_id, dtype_id);
    nerrors += test_map_get_count(file_id, MAP_COMP_COMP_NAME, NUMB_KEYS + 1, TRUE);
    nerrors += test_map_delete_key(file_id, MAP_COMP_COMP_NAME, dtype_id);
    nerrors += test_map_get_count(file_id, MAP_COMP_COMP_NAME, NUMB_KEYS, TRUE);

    if (H5Tclose(dtype_id) < 0)
        goto error;

    if (H5Pclose(mcpl_id) < 0)
        goto error;
    if (H5Pclose(mapl_id) < 0)
        goto error;

    return nerrors;

error:
    return ++nerrors;
}

static int
test_nested_compound(hid_t file_id)
{
    hid_t mcpl_id = -1, mapl_id = -1;
    hid_t dtype_id = -1, key_dtype_id = -1, vl_dtype_id = -1;
    int   i, j;
    int   nerrors = 0;

    TESTING("nested compound as the datatype of keys and values");
    HDputs("");

    vl_dtype_id = H5Tvlen_create(H5T_NATIVE_INT);

    dtype_id = H5Tcreate(H5T_COMPOUND, sizeof(nested_compound_t));
    H5Tinsert(dtype_id, "a_name", HOFFSET(nested_compound_t, a), H5T_NATIVE_INT);
    H5Tinsert(dtype_id, "b_name", HOFFSET(nested_compound_t, b), H5T_NATIVE_FLOAT);
    H5Tinsert(dtype_id, "c_name", HOFFSET(nested_compound_t, c), vl_dtype_id);

    /* Use different key datatype since vlens within a compound is not supported
     * for the key datatype */
    key_dtype_id = H5Tcreate(H5T_COMPOUND, sizeof(compound_t));
    H5Tinsert(key_dtype_id, "a_name", HOFFSET(compound_t, a), H5T_NATIVE_INT);
    H5Tinsert(key_dtype_id, "b_name", HOFFSET(compound_t, b), H5T_NATIVE_FLOAT);

    /* Generate random keys and values */
    for (i = 0; i < NUMB_KEYS; i++) {
        nested_comp_keys[i].a = random_base + i;
        nested_comp_keys[i].b = (float)(random_base + i * i);

        nested_comp_vals[i].a = rand();
        nested_comp_vals[i].b = (float)rand();

        nested_comp_vals[i].c.p   = malloc((size_t)(i + NUMB_KEYS) * sizeof(int));
        nested_comp_vals[i].c.len = (size_t)(i + NUMB_KEYS);
        for (j = 0; j < (i + NUMB_KEYS); j++)
            ((int *)nested_comp_vals[i].c.p)[j] = random_base + j;
    } /* end for */

    /* Create property lists */
    if ((mcpl_id = H5Pcreate(H5P_MAP_CREATE)) < 0)
        goto error;
    if ((mapl_id = H5Pcreate(H5P_MAP_ACCESS)) < 0)
        goto error;

    nerrors += test_create_map(file_id, MAP_NESTED_COMP_NAME, key_dtype_id, dtype_id, mcpl_id, mapl_id, TRUE);
    nerrors += test_open_map(file_id, MAP_NESTED_COMP_NAME, key_dtype_id, dtype_id, mcpl_id, mapl_id, TRUE);
    nerrors += test_map_set(file_id, MAP_NESTED_COMP_NAME, key_dtype_id, dtype_id, TRUE);
    nerrors += test_map_get_count(file_id, MAP_NESTED_COMP_NAME, NUMB_KEYS, TRUE);
    nerrors += test_map_get(file_id, MAP_NESTED_COMP_NAME, key_dtype_id, dtype_id, TRUE);
    nerrors += test_map_exists(file_id, MAP_NESTED_COMP_NAME, key_dtype_id);
    nerrors += test_map_iterate(file_id, MAP_NESTED_COMP_NAME, key_dtype_id);
    nerrors += test_map_update(file_id, MAP_NESTED_COMP_NAME, key_dtype_id, dtype_id);
    nerrors += test_map_nonexistent_key(file_id, MAP_NESTED_COMP_NAME, key_dtype_id, dtype_id);
    nerrors += test_map_get_count(file_id, MAP_NESTED_COMP_NAME, NUMB_KEYS + 1, TRUE);
    nerrors += test_map_delete_key(file_id, MAP_NESTED_COMP_NAME, key_dtype_id);
    nerrors += test_map_get_count(file_id, MAP_NESTED_COMP_NAME, NUMB_KEYS, TRUE);

    if (H5Tclose(dtype_id) < 0)
        goto error;

    if (H5Pclose(mcpl_id) < 0)
        goto error;
    if (H5Pclose(mapl_id) < 0)
        goto error;

    /* Free VL data */
    for (i = 0; i < NUMB_KEYS; i++)
        free(nested_comp_vals[i].c.p);

    return nerrors;

error:
    return ++nerrors;
}

static int
test_simple_tconv(hid_t file_id)
{
    hid_t mcpl_id = -1, mapl_id = -1;
    int   i, j;
    int   nerrors = 0;

    TESTING("simple type conversion");
    HDputs("");

    /* Generate random keys and values */
    for (i = 0; i < NUMB_KEYS; i++) {
        do {
            stconv_int_keys[i] = rand();
            for (j = 0; j < i; j++)
                if (stconv_int_keys[i] == stconv_int_keys[j])
                    break;
        } while (j < i);
        do {
            stconv_long_long_keys[i] = (long long)rand();
            for (j = 0; j < i; j++)
                if (stconv_long_long_keys[i] == stconv_long_long_keys[j])
                    break;
        } while (j < i);
        stconv_int_vals[i]    = rand();
        stconv_double_vals[i] = (double)rand();
    } /* end for */

    /* Create property lists */
    if ((mcpl_id = H5Pcreate(H5P_MAP_CREATE)) < 0)
        goto error;
    if ((mapl_id = H5Pcreate(H5P_MAP_ACCESS)) < 0)
        goto error;

    nerrors += test_create_map(file_id, MAP_SIMPLE_TCONV1_NAME, H5T_NATIVE_LLONG, H5T_NATIVE_DOUBLE, mcpl_id,
                               mapl_id, TRUE);
    nerrors += test_open_map(file_id, MAP_SIMPLE_TCONV1_NAME, H5T_NATIVE_LLONG, H5T_NATIVE_DOUBLE, mcpl_id,
                             mapl_id, TRUE);
    nerrors += test_map_set(file_id, MAP_SIMPLE_TCONV1_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT, TRUE);
    nerrors += test_map_get_count(file_id, MAP_SIMPLE_TCONV1_NAME, NUMB_KEYS, TRUE);
    nerrors += test_map_get(file_id, MAP_SIMPLE_TCONV1_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT, TRUE);
    nerrors += test_map_exists(file_id, MAP_SIMPLE_TCONV1_NAME, H5T_NATIVE_INT);
    nerrors += test_map_iterate(file_id, MAP_SIMPLE_TCONV1_NAME, H5T_NATIVE_INT);
    nerrors += test_map_update(file_id, MAP_SIMPLE_TCONV1_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT);
    nerrors += test_map_nonexistent_key(file_id, MAP_SIMPLE_TCONV1_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT);
    nerrors += test_map_get_count(file_id, MAP_SIMPLE_TCONV1_NAME, NUMB_KEYS + 1, TRUE);
    nerrors += test_map_delete_key(file_id, MAP_SIMPLE_TCONV1_NAME, H5T_NATIVE_INT);
    nerrors += test_map_get_count(file_id, MAP_SIMPLE_TCONV1_NAME, NUMB_KEYS, TRUE);

    nerrors += test_create_map(file_id, MAP_SIMPLE_TCONV2_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT, mcpl_id,
                               mapl_id, TRUE);
    nerrors += test_open_map(file_id, MAP_SIMPLE_TCONV2_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT, mcpl_id,
                             mapl_id, TRUE);
    nerrors += test_map_set(file_id, MAP_SIMPLE_TCONV2_NAME, H5T_NATIVE_LLONG, H5T_NATIVE_DOUBLE, TRUE);
    nerrors += test_map_get_count(file_id, MAP_SIMPLE_TCONV2_NAME, NUMB_KEYS, TRUE);
    nerrors += test_map_get(file_id, MAP_SIMPLE_TCONV2_NAME, H5T_NATIVE_LLONG, H5T_NATIVE_DOUBLE, TRUE);
    nerrors += test_map_exists(file_id, MAP_SIMPLE_TCONV2_NAME, H5T_NATIVE_LLONG);
    nerrors += test_map_iterate(file_id, MAP_SIMPLE_TCONV2_NAME, H5T_NATIVE_LLONG);
    nerrors += test_map_update(file_id, MAP_SIMPLE_TCONV2_NAME, H5T_NATIVE_LLONG, H5T_NATIVE_DOUBLE);
    nerrors += test_map_nonexistent_key(file_id, MAP_SIMPLE_TCONV2_NAME, H5T_NATIVE_LLONG, H5T_NATIVE_DOUBLE);
    nerrors += test_map_get_count(file_id, MAP_SIMPLE_TCONV2_NAME, NUMB_KEYS + 1, TRUE);
    nerrors += test_map_delete_key(file_id, MAP_SIMPLE_TCONV2_NAME, H5T_NATIVE_LLONG);
    nerrors += test_map_get_count(file_id, MAP_SIMPLE_TCONV2_NAME, NUMB_KEYS, TRUE);

    if (H5Pclose(mcpl_id) < 0)
        goto error;
    if (H5Pclose(mapl_id) < 0)
        goto error;

    return nerrors;

error:
    return ++nerrors;
}

static int
test_vl_tconv(hid_t file_id)
{
    hid_t mcpl_id = -1, mapl_id = -1;
    int   i, j;
    int   nerrors = 0;

    TESTING("type conversion of variable-length parent type");
    HDputs("");

    vl_int_dtype_id       = H5Tvlen_create(H5T_NATIVE_INT);
    vl_long_long_dtype_id = H5Tvlen_create(H5T_NATIVE_LLONG);
    vl_double_dtype_id    = H5Tvlen_create(H5T_NATIVE_DOUBLE);

    /* Allocate and initialize VL data to keys and values */
    for (i = 0; i < NUMB_KEYS; i++) {
        vl_int_keys[i].p   = malloc((size_t)(i + 1) * sizeof(int));
        vl_int_keys[i].len = (size_t)(i + 1);
        for (j = 0; j < (i + 1); j++)
            ((int *)(vl_int_keys[i].p))[j] = (int)(i * 10 + j + 7);

        vl_long_long_keys[i].p   = malloc((size_t)(i + 1) * sizeof(long long));
        vl_long_long_keys[i].len = (size_t)(i + 1);
        for (j = 0; j < (i + 1); j++)
            ((long long *)(vl_long_long_keys[i].p))[j] = (long long)(i * 10 + j + 7);

        vl_int_vals[i].p   = malloc((size_t)(i + NUMB_KEYS) * sizeof(int));
        vl_int_vals[i].len = (size_t)(i + NUMB_KEYS);
        for (j = 0; j < (i + NUMB_KEYS); j++)
            ((int *)vl_int_vals[i].p)[j] = random_base + j;

        vl_double_vals[i].p   = malloc((size_t)(i + NUMB_KEYS) * sizeof(double));
        vl_double_vals[i].len = (size_t)(i + NUMB_KEYS);
        for (j = 0; j < (i + NUMB_KEYS); j++)
            ((double *)vl_double_vals[i].p)[j] = (double)(random_base + j + 77);
    } /* end for */

    /* Create property lists */
    if ((mcpl_id = H5Pcreate(H5P_MAP_CREATE)) < 0)
        goto error;
    if ((mapl_id = H5Pcreate(H5P_MAP_ACCESS)) < 0)
        goto error;

    nerrors += test_create_map(file_id, MAP_VL_TCONV1_NAME, vl_long_long_dtype_id, vl_double_dtype_id,
                               mcpl_id, mapl_id, TRUE);
    nerrors += test_open_map(file_id, MAP_VL_TCONV1_NAME, vl_long_long_dtype_id, vl_double_dtype_id, mcpl_id,
                             mapl_id, TRUE);
    nerrors += test_map_set(file_id, MAP_VL_TCONV1_NAME, vl_int_dtype_id, vl_int_dtype_id, TRUE);
    nerrors += test_map_get_count(file_id, MAP_VL_TCONV1_NAME, NUMB_KEYS, TRUE);
    nerrors += test_map_get(file_id, MAP_VL_TCONV1_NAME, vl_int_dtype_id, vl_int_dtype_id, TRUE);
    nerrors += test_map_exists(file_id, MAP_VL_TCONV1_NAME, vl_int_dtype_id);
    nerrors += test_map_iterate(file_id, MAP_VL_TCONV1_NAME, vl_int_dtype_id);
    nerrors += test_map_update(file_id, MAP_VL_TCONV1_NAME, vl_int_dtype_id, vl_int_dtype_id);
    nerrors += test_map_nonexistent_key(file_id, MAP_VL_TCONV1_NAME, vl_int_dtype_id, vl_int_dtype_id);
    nerrors += test_map_get_count(file_id, MAP_VL_TCONV1_NAME, NUMB_KEYS + 1, TRUE);
    nerrors += test_map_delete_key(file_id, MAP_VL_TCONV1_NAME, vl_int_dtype_id);
    nerrors += test_map_get_count(file_id, MAP_VL_TCONV1_NAME, NUMB_KEYS, TRUE);

    nerrors += test_create_map(file_id, MAP_VL_TCONV2_NAME, vl_int_dtype_id, vl_int_dtype_id, mcpl_id,
                               mapl_id, TRUE);
    nerrors +=
        test_open_map(file_id, MAP_VL_TCONV2_NAME, vl_int_dtype_id, vl_int_dtype_id, mcpl_id, mapl_id, TRUE);
    nerrors += test_map_set(file_id, MAP_VL_TCONV2_NAME, vl_long_long_dtype_id, vl_double_dtype_id, TRUE);
    nerrors += test_map_get_count(file_id, MAP_VL_TCONV2_NAME, NUMB_KEYS, TRUE);
    nerrors += test_map_get(file_id, MAP_VL_TCONV2_NAME, vl_long_long_dtype_id, vl_double_dtype_id, TRUE);
    nerrors += test_map_exists(file_id, MAP_VL_TCONV2_NAME, vl_long_long_dtype_id);
    nerrors += test_map_iterate(file_id, MAP_VL_TCONV2_NAME, vl_long_long_dtype_id);
    nerrors += test_map_update(file_id, MAP_VL_TCONV2_NAME, vl_long_long_dtype_id, vl_double_dtype_id);
    nerrors +=
        test_map_nonexistent_key(file_id, MAP_VL_TCONV2_NAME, vl_long_long_dtype_id, vl_double_dtype_id);
    nerrors += test_map_get_count(file_id, MAP_VL_TCONV2_NAME, NUMB_KEYS + 1, TRUE);
    nerrors += test_map_delete_key(file_id, MAP_VL_TCONV2_NAME, vl_long_long_dtype_id);
    nerrors += test_map_get_count(file_id, MAP_VL_TCONV2_NAME, NUMB_KEYS, TRUE);

    if (H5Tclose(vl_int_dtype_id) < 0)
        goto error;
    if (H5Tclose(vl_long_long_dtype_id) < 0)
        goto error;
    if (H5Tclose(vl_double_dtype_id) < 0)
        goto error;

    if (H5Pclose(mcpl_id) < 0)
        goto error;
    if (H5Pclose(mapl_id) < 0)
        goto error;

    /* Free VL data */
    for (i = 0; i < NUMB_KEYS; i++) {
        free(vl_int_keys[i].p);
        free(vl_long_long_keys[i].p);
        free(vl_int_vals[i].p);
        free(vl_double_vals[i].p);
    } /* end for */

    return nerrors;

error:
    return ++nerrors;
}

static int
test_many_entries(hid_t file_id)
{
    hid_t mcpl_id = -1, mapl_id = -1;
    int   i;
    int   nerrors = 0;

    TESTING("large number of entries of keys and values");
    HDputs("");

    /* Generate random keys and values */
    for (i = 0; i < LARGE_NUMB_KEYS; i++) {
        large_int_int_keys[i] = (rand() % (256 * 256 * 256 * 32 / LARGE_NUMB_KEYS)) * LARGE_NUMB_KEYS + i;
        large_int_int_vals[i] = rand();
    } /* end for */

    /* Create property lists */
    if ((mcpl_id = H5Pcreate(H5P_MAP_CREATE)) < 0)
        goto error;
    if ((mapl_id = H5Pcreate(H5P_MAP_ACCESS)) < 0)
        goto error;

    nerrors += test_create_map(file_id, MAP_MANY_ENTRIES_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT, mcpl_id,
                               mapl_id, TRUE);
    nerrors +=
        test_open_map(file_id, MAP_MANY_ENTRIES_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT, mcpl_id, mapl_id, TRUE);
    nerrors += test_map_set(file_id, MAP_MANY_ENTRIES_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT, TRUE);
    nerrors += test_map_get_count(file_id, MAP_MANY_ENTRIES_NAME, LARGE_NUMB_KEYS, TRUE);
    nerrors += test_map_get(file_id, MAP_MANY_ENTRIES_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT, TRUE);
    nerrors += test_map_exists(file_id, MAP_MANY_ENTRIES_NAME, H5T_NATIVE_INT);
    nerrors += test_map_iterate(file_id, MAP_MANY_ENTRIES_NAME, H5T_NATIVE_INT);
    nerrors += test_map_delete_key(file_id, MAP_MANY_ENTRIES_NAME, H5T_NATIVE_INT);
    nerrors += test_map_get_count(file_id, MAP_MANY_ENTRIES_NAME, LARGE_NUMB_KEYS - 1, TRUE);

    if (H5Pclose(mcpl_id) < 0)
        goto error;
    if (H5Pclose(mapl_id) < 0)
        goto error;

    return nerrors;

error:
    return ++nerrors;
}

/*
 * Tests creating many map objects
 */
static int
test_many_maps(hid_t file_id)
{
    hid_t mcpl_id = -1, mapl_id = -1;
    char  map_name[64];
    int   i;
    int   nerrors = 0;

    TESTING("large number of map objects");
    HDputs("");

    /* Create property lists */
    if ((mcpl_id = H5Pcreate(H5P_MAP_CREATE)) < 0)
        goto error;
    if ((mapl_id = H5Pcreate(H5P_MAP_ACCESS)) < 0)
        goto error;

    TESTING_2("creation of map object");
    for (i = 0; i < LARGE_NUMB_MAPS; i++) {
        sprintf(map_name, "map_large_name_%d", i);
        nerrors +=
            test_create_map(file_id, map_name, H5T_NATIVE_INT, H5T_NATIVE_INT, mcpl_id, mapl_id, FALSE);

        if (nerrors) {
            H5_FAILED();
            AT();
            printf("     failed to create many maps\n");
            goto error;
        }
    }
    PASSED();
    fflush(stdout);

    TESTING_2("open of map object");
    for (i = 0; i < LARGE_NUMB_MAPS; i++) {
        sprintf(map_name, "map_large_name_%d", i);
        nerrors += test_open_map(file_id, map_name, H5T_NATIVE_INT, H5T_NATIVE_INT, mcpl_id, mapl_id, FALSE);

        if (nerrors) {
            H5_FAILED();
            AT();
            printf("     failed to open many maps\n");
            goto error;
        }
    }
    PASSED();
    fflush(stdout);

    TESTING_2("map set with keys and values");
    for (i = 0; i < LARGE_NUMB_MAPS; i++) {
        sprintf(map_name, "map_large_name_%d", i);
        nerrors += test_map_set(file_id, map_name, H5T_NATIVE_INT, H5T_NATIVE_INT, FALSE);

        if (nerrors) {
            H5_FAILED();
            AT();
            printf("     failed to set with keys and values\n");
            goto error;
        }

        nerrors += test_map_get_count(file_id, map_name, NUMB_KEYS, FALSE);

        if (nerrors) {
            H5_FAILED();
            AT();
            printf("     H5Mget_count() test failed after set\n");
            goto error;
        }
    }
    PASSED();
    fflush(stdout);

    TESTING_2("map get with keys and values");
    for (i = 0; i < LARGE_NUMB_MAPS; i++) {
        sprintf(map_name, "map_large_name_%d", i);
        nerrors += test_map_get(file_id, map_name, H5T_NATIVE_INT, H5T_NATIVE_INT, FALSE);

        if (nerrors) {
            H5_FAILED();
            AT();
            printf("     failed to get with keys and values\n");
            goto error;
        }
    }
    PASSED();
    fflush(stdout);

    if (H5Pclose(mcpl_id) < 0)
        goto error;
    if (H5Pclose(mapl_id) < 0)
        goto error;

    return nerrors;

error:
    return ++nerrors;
} /* end test_many_maps() */

/*
 * Tests opening a non-existent map object
 */
static int
test_nonexistent_map(hid_t file_id)
{
    hid_t map_id = -1;

    TESTING("opening a non-existent map object");

    H5E_BEGIN_TRY
    {
        map_id = H5Mopen(file_id, MAP_NONEXISTENT_MAP, H5P_DEFAULT);
    }
    H5E_END_TRY;

    if (map_id >= 0) {
        H5_FAILED();
        AT();
        printf("    shouldn't open the nonexistent map\n");
        goto error;
    } /* end if */

    PASSED();
    fflush(stdout);

    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Mclose(map_id);
    }
    H5E_END_TRY;

    return 1;
} /* end test_nonexistent_map() */

/*
 * main function
 */
int
main(int argc, char **argv)
{
    hid_t fapl_id = -1, file_id = -1;
    int   nerrors = 0;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

    srand((unsigned)time(NULL));
    random_base = rand();

    if ((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0) {
        nerrors++;
        goto error;
    }

    if (H5Pset_all_coll_metadata_ops(fapl_id, true) < 0) {
        nerrors++;
        goto error;
    }

    if ((file_id = H5Fcreate(FILENAME, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0) {
        nerrors++;
        goto error;
    }

    if ((scalar_space_id = H5Screate(H5S_SCALAR)) < 0) {
        nerrors++;
        goto error;
    }

    nerrors += test_integer(file_id);
    nerrors += test_enum(file_id);
    nerrors += test_compound(file_id);
    nerrors += test_vl(file_id);
    nerrors += test_vls(file_id);
    nerrors += test_nested_compound(file_id);
    nerrors += test_simple_tconv(file_id);
    nerrors += test_vl_tconv(file_id);
    nerrors += test_many_entries(file_id);
    nerrors += test_many_maps(file_id);
    nerrors += test_nonexistent_map(file_id);

    if (H5Pclose(fapl_id) < 0) {
        nerrors++;
        goto error;
    }

    if (H5Fclose(file_id) < 0) {
        nerrors++;
        goto error;
    }

    if (H5Sclose(scalar_space_id) < 0) {
        nerrors++;
        goto error;
    }

    if (nerrors)
        goto error;

    if (MAINPROCESS)
        puts("All DAOS Map tests passed");

    MPI_Finalize();

    return 0;

error:
    if (MAINPROCESS)
        printf("*** %d TEST%s FAILED ***\n", nerrors, (!nerrors || nerrors > 1) ? "S" : "");

    MPI_Finalize();

    return 1;
} /* end main() */
