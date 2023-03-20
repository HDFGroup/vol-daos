/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "h5dsm_example.h"
#include <time.h>

int
main(int argc, char *argv[])
{
    char   *daos_sys = NULL;
    hid_t   file = -1, dset = -1, space = -1, fapl = -1;
    hid_t   nfile = -1, ndset = -1;
    hsize_t dims[1] = {256 * 1024};
    size_t  i;
#ifdef DV_HAVE_SNAP_OPEN_ID
    H5_daos_snap_id_t snap_id;
#endif
    int *buf = NULL;

    (void)MPI_Init(&argc, &argv);

    /* Seed random number generator */
    srand(time(NULL));

    if (argc < 4 || argc > 5)
        PRINTF_ERROR("argc must be 4 or 5\n");

    /* Set up FAPL */
    if ((fapl = H5Pcreate(H5P_FILE_ACCESS)) < 0)
        ERROR;
    if (H5Pset_mpi_params(fapl, MPI_COMM_WORLD, MPI_INFO_NULL) < 0)
        ERROR;
    if (H5Pset_fapl_daos(fapl, argv[1], daos_sys) < 0)
        ERROR;
    if (H5Pset_all_coll_metadata_ops(fapl, true) < 0)
        ERROR;

    /* Create file */
    if ((file = H5Fcreate(argv[2], H5F_ACC_TRUNC, H5P_DEFAULT, fapl)) < 0)
        ERROR;

    /* Create native file */
    if ((nfile = H5Fcreate(argv[2], H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT)) < 0)
        ERROR;

    /* Set up dataspace */
    if ((space = H5Screate_simple(1, dims, NULL)) < 0)
        ERROR;

    /* Create dataset */
    if ((dset = H5Dcreate2(file, argv[3], H5T_NATIVE_INT, space, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0)
        ERROR;

    /* Create native dataset */
    if ((ndset = H5Dcreate2(nfile, argv[3], H5T_NATIVE_INT, space, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) <
        0)
        ERROR;

    /* Allocate buffer */
    if (NULL == (buf = (int *)malloc(dims[0] * sizeof(int))))
        ERROR;

    /* Fill buffer */
    for (i = 0; (hsize_t)i < dims[0]; i++)
        buf[i] = rand();

    /* Write data */
    printf("Writing daos dataset\n");
    if (H5Dwrite(dset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, buf) < 0)
        ERROR;

    /* Write native data */
    printf("Writing native dataset\n");
    if (H5Dwrite(ndset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, buf) < 0)
        ERROR;

#ifdef DV_HAVE_SNAP_OPEN_ID
    /* Save snapshot if requested */
    if (argc == 5) {
        if (H5daos_snap_create(file, &snap_id) < 0)
            ERROR;
        printf("Saved snapshot: snap_id = %llu\n", (long long unsigned)snap_id);
    } /* end if */
#endif

    /* Close */
    if (H5Dclose(dset) < 0)
        ERROR;
    if (H5Dclose(ndset) < 0)
        ERROR;
    if (H5Fclose(file) < 0)
        ERROR;
    if (H5Fclose(nfile) < 0)
        ERROR;
    if (H5Sclose(space) < 0)
        ERROR;
    if (H5Pclose(fapl) < 0)
        ERROR;
    free(buf);
    buf = NULL;

    printf("Success\n");

    (void)MPI_Finalize();
    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Dclose(dset);
        H5Dclose(ndset);
        H5Fclose(file);
        H5Fclose(nfile);
        H5Sclose(space);
        H5Pclose(fapl);
    }
    H5E_END_TRY;

    free(buf);

    (void)MPI_Finalize();
    return 1;
}
