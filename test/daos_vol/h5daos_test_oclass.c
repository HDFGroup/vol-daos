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
 * Purpose: Tests object class routines in the DAOS VOL connector
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <hdf5.h>

#include "daos_vol_public.h"
#include "h5daos_test.h"

/*
 * Definitions
 */
#define TRUE                    1
#define FALSE                   0

#define FILENAME                "h5daos_test_oclass.h5"

#define DEF_GROUP_NAME          "def_group"
#define MOD_GROUP_NAME          "mod_group"
#define DEF_DSET_NAME           "def_dset"
#define MOD_DSET_NAME           "mod_dset"

/*
 * Global variables
 */
uuid_t pool_uuid;
int    mpi_rank;

int check_plist_oclass(hid_t plist_id, const char *exp_oclass);
int test_oclass(hid_t fcpl_id, hid_t fapl_id, const char *exp_root_oclass,
    const char *exp_group_oclass, const char *exp_dset_oclass);

/*
 * Function to check object class on a property list
 */
int
check_plist_oclass(hid_t plist_id, const char *exp_oclass)
{
    ssize_t oclass_size;
    char oclass_buf[10];

    if((oclass_size = H5daos_get_object_class(plist_id, oclass_buf, sizeof(oclass_buf))) < 0)
        TEST_ERROR
    if(oclass_size != (ssize_t)strlen(oclass_buf)) {
        H5_FAILED() AT()
        printf("incorrect object class size\n");
        goto error;
    } /* end if */
    if(strcmp(oclass_buf, exp_oclass)) {
        H5_FAILED() AT()
        printf("returned object class \"%s\" does not match expected: \"%s\"\n", oclass_buf, exp_oclass);
        goto error;
    } /* end if */

    return 0;

error:
    return 1;
} /* end check_plist_oclass() */

/*
 * Test function
 */
int
test_oclass(hid_t fcpl_id, hid_t fapl_id, const char *exp_root_oclass,
    const char *exp_group_oclass, const char *exp_dset_oclass)
{
    hid_t file_id = -1;
    hid_t obj_id = -1;
    hid_t plist_id = -1;
    hid_t space_id = -1;
    hsize_t dims[1] = {1};
    int i;

    /* Create dataspace */
    if((space_id = H5Screate_simple(1, dims, NULL)) < 0)
        TEST_ERROR

    /* Create file */
    if((file_id = H5Fcreate(FILENAME, H5F_ACC_TRUNC, fcpl_id, fapl_id)) < 0)
        TEST_ERROR

    /* Create default group */
    if((obj_id = H5Gcreate2(file_id, DEF_GROUP_NAME, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0)
        TEST_ERROR

    /* Check group's object class */
    if((plist_id = H5Gget_create_plist(obj_id)) < 0)
        TEST_ERROR
    if(0 != check_plist_oclass(plist_id, exp_group_oclass)) {
        H5_FAILED() AT()
        printf("object class check failed for default group\n");
        goto error;
    } /* end if */
    if(H5Pclose(plist_id) < 0)
        TEST_ERROR
    plist_id = -1;

    if(H5Gclose(obj_id) < 0)
        TEST_ERROR
    obj_id = -1;

    /* Create custom group */
    if((plist_id = H5Pcreate(H5P_GROUP_CREATE)) < 0)
        TEST_ERROR
    if(H5daos_set_object_class(plist_id, "S2") < 0)
        TEST_ERROR
    if((obj_id = H5Gcreate2(file_id, MOD_GROUP_NAME, H5P_DEFAULT, plist_id, H5P_DEFAULT)) < 0)
        TEST_ERROR
    if(H5Pclose(plist_id) < 0)
        TEST_ERROR
    plist_id = -1;

    /* Check group's object class */
    if((plist_id = H5Gget_create_plist(obj_id)) < 0)
        TEST_ERROR
    if(0 != check_plist_oclass(plist_id, "S2")) {
        H5_FAILED() AT()
        printf("object class check failed for custom group\n");
        goto error;
    } /* end if */
    if(H5Pclose(plist_id) < 0)
        TEST_ERROR
    plist_id = -1;

    if(H5Gclose(obj_id) < 0)
        TEST_ERROR
    obj_id = -1;

    /* Create default dataset */
    if((obj_id = H5Dcreate2(file_id, DEF_DSET_NAME, H5T_NATIVE_INT, space_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0)
        TEST_ERROR

    /* Check dataset's object class */
    if((plist_id = H5Dget_create_plist(obj_id)) < 0)
        TEST_ERROR
    /** SX is translated to the largest number of targets available on a system. skip this for now */
#if 0
    if(0 != check_plist_oclass(plist_id, exp_dset_oclass)) {
        H5_FAILED() AT()
        printf("object class check failed for default dataset\n");
        goto error;
    } /* end if */
#else
    (void)exp_dset_oclass; /* silence compiler */
#endif
    if(H5Pclose(plist_id) < 0)
        TEST_ERROR
    plist_id = -1;

    if(H5Dclose(obj_id) < 0)
        TEST_ERROR
    obj_id = -1;

    /* Create custom dataset */
    if((plist_id = H5Pcreate(H5P_DATASET_CREATE)) < 0)
        TEST_ERROR
    if(H5daos_set_object_class(plist_id, "S1") < 0)
        TEST_ERROR
    if((obj_id = H5Dcreate2(file_id, MOD_DSET_NAME, H5T_NATIVE_INT, space_id, H5P_DEFAULT, plist_id, H5P_DEFAULT)) < 0)
        TEST_ERROR
    if(H5Pclose(plist_id) < 0)
        TEST_ERROR
    plist_id = -1;

    /* Check dataset's object class */
    if((plist_id = H5Dget_create_plist(obj_id)) < 0)
        TEST_ERROR
    if(0 != check_plist_oclass(plist_id, "S1")) {
        H5_FAILED() AT()
        printf("object class check failed for custom dataset\n");
        goto error;
    } /* end if */
    if(H5Pclose(plist_id) < 0)
        TEST_ERROR
    plist_id = -1;

    if(H5Dclose(obj_id) < 0)
        TEST_ERROR
    obj_id = -1;

    /* Check all object classes after reopening objects, both before and after
     * reopening the file */
    for(i = 0; i < 2; i++) {
        /* Check file's object class */
        if((plist_id = H5Fget_create_plist(file_id)) < 0)
            TEST_ERROR
        if(0 != check_plist_oclass(plist_id, exp_root_oclass)) {
            H5_FAILED() AT()
            printf("object class check failed for file%s\n", i ? " after file reopen" : "");
            goto error;
        } /* end if */
        if(H5Pclose(plist_id) < 0)
            TEST_ERROR
        plist_id = -1;

        /* Open root group and check its object class */
        if((obj_id = H5Gopen2(file_id, "/", H5P_DEFAULT)) < 0)
            TEST_ERROR
        if((plist_id = H5Gget_create_plist(obj_id)) < 0)
            TEST_ERROR
        if(0 != check_plist_oclass(plist_id, exp_root_oclass)) {
            H5_FAILED() AT()
            printf("object class check failed for root group%s\n", i ? " after file reopen" : "");
            goto error;
        } /* end if */
        if(H5Pclose(plist_id) < 0)
            TEST_ERROR
        plist_id = -1;
        if(H5Gclose(obj_id) < 0)
            TEST_ERROR
        obj_id = -1;

        /* Open default group and check its object class */
        if((obj_id = H5Gopen2(file_id, DEF_GROUP_NAME, H5P_DEFAULT)) < 0)
            TEST_ERROR
        if((plist_id = H5Gget_create_plist(obj_id)) < 0)
            TEST_ERROR
        if(0 != check_plist_oclass(plist_id, exp_group_oclass)) {
            H5_FAILED() AT()
            printf("object class check failed for default group after %s reopen", i ? "file" : "object");
            goto error;
        } /* end if */
        if(H5Pclose(plist_id) < 0)
            TEST_ERROR
        plist_id = -1;
        if(H5Gclose(obj_id) < 0)
            TEST_ERROR
        obj_id = -1;

        /* Open custom group and check its object class */
        if((obj_id = H5Gopen2(file_id, MOD_GROUP_NAME, H5P_DEFAULT)) < 0)
            TEST_ERROR
        if((plist_id = H5Gget_create_plist(obj_id)) < 0)
            TEST_ERROR
        if(0 != check_plist_oclass(plist_id, "S2")) {
            H5_FAILED() AT()
            printf("object class check failed for custom group after %s reopen", i ? "file" : "object");
            goto error;
        } /* end if */
        if(H5Pclose(plist_id) < 0)
            TEST_ERROR
        plist_id = -1;
        if(H5Gclose(obj_id) < 0)
            TEST_ERROR
        obj_id = -1;

        /* Open default dataset and check its object class */
        if((obj_id = H5Dopen2(file_id, DEF_DSET_NAME, H5P_DEFAULT)) < 0)
            TEST_ERROR
        if((plist_id = H5Dget_create_plist(obj_id)) < 0)
            TEST_ERROR
        /** SX is translated to the largest number of targets available on a system. skip this for now */
#if 0
        if(0 != check_plist_oclass(plist_id, exp_dset_oclass)) {
            H5_FAILED() AT()
            printf("object class check failed for default dataset after %s reopen", i ? "file" : "object");
            goto error;
        } /* end if */
#endif
        if(H5Pclose(plist_id) < 0)
            TEST_ERROR
        plist_id = -1;
        if(H5Dclose(obj_id) < 0)
            TEST_ERROR
        obj_id = -1;

        /* Open custom dataset and check its object class */
        if((obj_id = H5Dopen2(file_id, MOD_DSET_NAME, H5P_DEFAULT)) < 0)
            TEST_ERROR
        if((plist_id = H5Dget_create_plist(obj_id)) < 0)
            TEST_ERROR
        if(0 != check_plist_oclass(plist_id, "S1")) {
            H5_FAILED() AT()
            printf("object class check failed for custom dataset after %s reopen", i ? "file" : "object");
            goto error;
        } /* end if */
        if(H5Pclose(plist_id) < 0)
            TEST_ERROR
        plist_id = -1;
        if(H5Dclose(obj_id) < 0)
            TEST_ERROR
        obj_id = -1;

        /* No need to reopen the file on the second iteration */
        if(i)
            break;

        /* Reopen file */
        if(H5Fclose(file_id) < 0)
            TEST_ERROR
        if((file_id = H5Fopen(FILENAME, H5F_ACC_RDONLY, fapl_id)) < 0)
            TEST_ERROR
    } /* end for */

    /* Close */
    if(H5Fclose(file_id) < 0)
        TEST_ERROR
    if(H5Sclose(space_id) < 0)
        TEST_ERROR

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Fclose(file_id);
        H5Sclose(space_id);
        H5Oclose(obj_id);
        H5Pclose(plist_id);
    } H5E_END_TRY;

    return 1;
} /* test_oclass() */

/*
 * main function
 */
int
main( int argc, char** argv )
{
    hid_t   fcpl_id = -1;
    hid_t   fapl_id = -1;
    int     nerrors = 0;

    MPI_Init(&argc, &argv);

    if((fcpl_id = H5Pcreate(H5P_FILE_CREATE)) < 0) {
        nerrors++;
        goto error;
    }

    if((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0) {
        nerrors++;
        goto error;
    }

    /** set RF0 property on container */
    if(H5daos_set_prop(fcpl_id, "rf:0") < 0) {
        nerrors++;
        goto error;
    }

    /* First test: both default */
    TESTING("default FCPL and FAPL");
    nerrors += test_oclass(fcpl_id, fapl_id, "S1", "S1", "SX");

    /* Second test: custom FCPL, default FAPL */
    TESTING("custom FCPL, default FAPL");
    if(H5daos_set_object_class(fcpl_id, "S2") < 0) {
        nerrors++;
        goto error;
    }
    if(H5daos_set_root_open_object_class(fapl_id, "S2") < 0) {
        nerrors++;
        goto error;
    }
    nerrors += test_oclass(fcpl_id, fapl_id, "S2", "S1", "SX");

    /* Second test: custom FCPL, custom FAPL */
    TESTING("custom FCPL and FAPL");
    if(H5daos_set_object_class(fapl_id, "S4") < 0) {
        nerrors++;
        goto error;
    }
    nerrors += test_oclass(fcpl_id, fapl_id, "S2", "S4", "S4");

    /* Second test: default FCPL, custom FAPL */
    TESTING("default FCPL, custom FAPL");
    if(H5daos_set_object_class(fcpl_id, NULL) < 0) {
        nerrors++;
        goto error;
    }
    if(H5daos_set_root_open_object_class(fapl_id, "S4") < 0) {
        nerrors++;
        goto error;
    }
    nerrors += test_oclass(fcpl_id, fapl_id, "S4", "S4", "S4");


    if(H5Pclose(fapl_id) < 0) {
        nerrors++;
        goto error;
    }

    if(H5Pclose(fcpl_id) < 0) {
        nerrors++;
        goto error;
    }

    if (nerrors) goto error;

    if (MAINPROCESS) puts("All DAOS object class tests passed");

    MPI_Finalize();

    return 0;

error:
    if (MAINPROCESS) printf("*** %d TEST%s FAILED ***\n", nerrors, (!nerrors || nerrors > 1) ? "S" : "");

    MPI_Finalize();

    return 1;
} /* end main() */
