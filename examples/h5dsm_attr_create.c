/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "h5dsm_example.h"

int
main(int argc, char *argv[])
{
    char   *daos_sys = NULL;
    hid_t   file = -1, attr = -1, space = -1, fapl = -1;
    hsize_t dims[2] = {4, 6};
#ifdef DV_HAVE_SNAP_OPEN_ID
    H5_daos_snap_id_t snap_id;
#endif

    (void)MPI_Init(&argc, &argv);

    if (argc < 5 || argc > 6)
        PRINTF_ERROR("argc must be 5 or 6\n");

    /* Set up FAPL */
    if ((fapl = H5Pcreate(H5P_FILE_ACCESS)) < 0)
        ERROR;
    if (H5Pset_mpi_params(fapl, MPI_COMM_WORLD, MPI_INFO_NULL) < 0)
        ERROR;
    if (H5Pset_fapl_daos(fapl, argv[1], daos_sys) < 0)
        ERROR;
    if (H5Pset_all_coll_metadata_ops(fapl, true) < 0)
        ERROR;

    /* Set up dataspace */
    if ((space = H5Screate_simple(2, dims, NULL)) < 0)
        ERROR;

    /* Open file */
    if ((file = H5Fopen(argv[2], H5F_ACC_RDWR, fapl)) < 0)
        ERROR;

    printf("Creating attribute\n");

    /* Create attribute */
    if ((attr = H5Acreate_by_name(file, argv[3], argv[4], H5T_NATIVE_INT, space, H5P_DEFAULT, H5P_DEFAULT,
                                  H5P_DEFAULT)) < 0)
        ERROR;

#ifdef DV_HAVE_SNAP_OPEN_ID
    /* Save snapshot if requested */
    if (argc == 6) {
        if (H5daos_snap_create(file, &snap_id) < 0)
            ERROR;
        printf("Saved snapshot: snap_id = %llu\n", (long long unsigned)snap_id);
    } /* end if */
#endif

    /* Close */
    if (H5Aclose(attr) < 0)
        ERROR;
    if (H5Fclose(file) < 0)
        ERROR;
    if (H5Sclose(space) < 0)
        ERROR;
    if (H5Pclose(fapl) < 0)
        ERROR;

    printf("Success\n");

    (void)MPI_Finalize();
    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Aclose(attr);
        H5Fclose(file);
        H5Sclose(space);
        H5Pclose(fapl);
    }
    H5E_END_TRY;

    (void)MPI_Finalize();
    return 1;
}
