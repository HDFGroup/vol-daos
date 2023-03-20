/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "h5dsm_example.h"
#include <mpi.h>
#include <time.h>

int
main(int argc, char *argv[])
{
    char   *daos_sys = NULL;
    hid_t   file = -1, dset = -1, file_space = -1, mem_space = -1, fapl = -1;
    hsize_t dims[2] = {4, 6};
    hsize_t start[2], count[2];
#ifdef DV_HAVE_SNAP_OPEN_ID
    H5_daos_snap_id_t snap_id;
#endif
    int   buf[4][6];
    int   rank, mpi_size;
    char *file_sel_str[2] = {"XXX...", "...XXX"};
    int   i, j;

    (void)MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);

    if (mpi_size > 2)
        PRINTF_ERROR("mpi_size > 2\n");

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

    /* Open dataset */
    if ((dset = H5Dopen2(file, argv[3], H5P_DEFAULT)) < 0)
        ERROR;

    if (rank == 1)
        MPI_Barrier(MPI_COMM_WORLD);

    printf("---------------Rank %d---------------\n", rank);
    printf("Selecting elements denoted with X\n");
    printf("Memory File\n");
    printf("...... %s\n", file_sel_str[rank]);
    for (i = 1; i < 4; i++)
        printf(".XXXX. %s\n", file_sel_str[rank]);

    if (rank == 0)
        MPI_Barrier(MPI_COMM_WORLD);
    else
        printf("Reading dataset\n");

    MPI_Barrier(MPI_COMM_WORLD);

    /* Set up dataspaces */
    if ((file_space = H5Screate_simple(2, dims, NULL)) < 0)
        ERROR;
    if ((mem_space = H5Screate_simple(2, dims, NULL)) < 0)
        ERROR;
    start[0] = 0;
    start[1] = 3 * rank;
    count[0] = 4;
    count[1] = 3;
    if (H5Sselect_hyperslab(file_space, H5S_SELECT_SET, start, NULL, count, NULL) < 0)
        ERROR;
    start[0] = 1;
    start[1] = 1;
    count[0] = 3;
    count[1] = 4;
    if (H5Sselect_hyperslab(mem_space, H5S_SELECT_SET, start, NULL, count, NULL) < 0)
        ERROR;

    /* Initialize buffer */
    for (i = 0; i < 4; i++)
        for (j = 0; j < 6; j++)
            buf[i][j] = -1;

    /* Read data */
    if (H5Dread(dset, H5T_NATIVE_INT, mem_space, file_space, H5P_DEFAULT, buf) < 0)
        ERROR;

    if (rank == 1)
        MPI_Barrier(MPI_COMM_WORLD);

    /* Fill and print buffer */
    printf("---------------Rank %d---------------\n", rank);
    printf("Successfully read data. Buffer is:\n");
    for (i = 0; i < 4; i++) {
        for (j = 0; j < 6; j++)
            printf("%d ", buf[i][j]);
        printf("\n");
    }

    if (rank == 0)
        MPI_Barrier(MPI_COMM_WORLD);

    /* Close */
    if (H5Dclose(dset) < 0)
        ERROR;
    if (H5Fclose(file) < 0)
        ERROR;
    if (H5Sclose(file_space) < 0)
        ERROR;
    if (H5Sclose(mem_space) < 0)
        ERROR;
    if (H5Pclose(fapl) < 0)
        ERROR;

    printf("Success\n");

    (void)MPI_Finalize();
    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Dclose(dset);
        H5Fclose(file);
        H5Sclose(file_space);
        H5Sclose(mem_space);
        H5Pclose(fapl);
    }
    H5E_END_TRY;

    (void)MPI_Finalize();
    return 1;
}
