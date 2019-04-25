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
#include <hdf5.h>

#include "daos_vol_public.h"
#include "h5daos_test.h"

/*
 * Definitions
 */
#define FILENAME                "h5daos_test_map.h5"
#define MAP_INT_INT_NAME        "map_int_int"

/*
 * Global variables
 */
uuid_t pool_uuid;
int    mpi_rank;

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

    if((file_id = H5Fcreate(FILENAME, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0) {
        H5_FAILED();
        printf("    couldn't open file\n");
        goto error;
    }

    if((map_id = H5Mcreate(file_id, MAP_INT_INT_NAME, H5T_NATIVE_INT, H5T_NATIVE_INT, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
        H5_FAILED();
        printf("    couldn't create map\n");
        goto error;
    }

    if(H5Mclose(map_id) < 0)
        TEST_ERROR
    if(H5Pclose(fapl_id) < 0)
        TEST_ERROR
    if(H5Fclose(file_id) < 0)
        TEST_ERROR
#ifndef DAOS_INIT_FINI_BUG_WORKAROUND
    if(H5daos_term() < 0)
        TEST_ERROR
#endif

    PASSED();

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Mclose(map_id);
        H5Pclose(fapl_id);
        H5Fclose(file_id);
#ifndef DAOS_INIT_FINI_BUG_WORKAROUND
        H5daos_term();
#endif
    } H5E_END_TRY;

    return 1;
}

/*
 * main function
 */
int
main( int argc, char** argv )
{
    int     nerrors = 0;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

    nerrors += test_create_map();

    if (nerrors) goto error;

    if (MAINPROCESS) puts("All DAOS Map tests passed");

    MPI_Finalize();

    return 0;

error:
    if (MAINPROCESS) printf("*** %d TEST%s FAILED ***\n", nerrors, (!nerrors || nerrors > 1) ? "S" : "");

    MPI_Finalize();

    return 1;
}

