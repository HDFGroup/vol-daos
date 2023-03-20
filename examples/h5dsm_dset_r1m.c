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
    hid_t   file = -1, dset = -1, fapl = -1;
    hid_t   nfile = -1, ndset = -1;
    hsize_t dims[1] = {256 * 1024};
    int    *buf = NULL, *nbuf = NULL;
#ifdef DV_HAVE_SNAP_OPEN_ID
    H5_daos_snap_id_t snap_id;
#endif

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

#ifdef DV_HAVE_SNAP_OPEN_ID
    /* Open snapshot if specified */
    if (argc == 5) {
        snap_id = (H5_daos_snap_id_t)atoi(argv[4]);
        printf("Opening snapshot %llu\n", (long long unsigned)snap_id);
        if (H5Pset_daos_snap_open(fapl, snap_id) < 0)
            ERROR;
    } /* end if */
#endif

    /* Open file */
    if ((file = H5Fopen(argv[2], H5F_ACC_RDWR, fapl)) < 0)
        ERROR;

    /* Open native file */
    if ((nfile = H5Fopen(argv[2], H5F_ACC_RDWR, H5P_DEFAULT)) < 0)
        ERROR;

    /* Open dataset */
    if ((dset = H5Dopen2(file, argv[3], H5P_DEFAULT)) < 0)
        ERROR;

    /* Open native dataset */
    if ((ndset = H5Dopen2(nfile, argv[3], H5P_DEFAULT)) < 0)
        ERROR;

    /* Allocate buffers */
    if (NULL == (buf = (int *)malloc(dims[0] * sizeof(int))))
        ERROR;
    if (NULL == (nbuf = (int *)malloc(dims[0] * sizeof(int))))
        ERROR;

    /* Read data */
    printf("Reading daos dataset\n");
    if (H5Dread(dset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, buf) < 0)
        ERROR;

    /* Read native data */
    printf("Reading native dataset\n");
    if (H5Dread(ndset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, nbuf) < 0)
        ERROR;

    /* Compare data */
    if (memcmp(buf, nbuf, dims[0] * sizeof(int)))
        PRINTF_ERROR("Buffers differ\n");
    else
        printf("Buffers are equal\n");

    /* Close */
    if (H5Dclose(dset) < 0)
        ERROR;
    if (H5Dclose(ndset) < 0)
        ERROR;
    if (H5Fclose(file) < 0)
        ERROR;
    if (H5Fclose(nfile) < 0)
        ERROR;
    if (H5Pclose(fapl) < 0)
        ERROR;
    free(buf);
    buf = NULL;
    free(nbuf);
    nbuf = NULL;

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
        H5Pclose(fapl);
    }
    H5E_END_TRY;

    free(buf);
    free(nbuf);

    (void)MPI_Finalize();
    return 1;
}
