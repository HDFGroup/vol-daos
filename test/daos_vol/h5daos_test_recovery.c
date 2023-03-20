/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

/**
 * Purpose: Tests the recovery mechanism of DAOS
 */

#include "h5daos_test.h"

#include "daos_vol.h"

#include "daos_mgmt.h"
#include <cart/types.h>

/*
 * Definitions
 */
#define TRUE  1
#define FALSE 0

#define FILENAME         "h5daos_test_recovery.h5"
#define NAME_LENGTH      256
#define DATASETNAME1     "IntArray1"
#define DATASETNAME2     "IntArray2"
#define DATA_RANK        2
#define ARRAY_DTYPE_RANK 1
#define ATTR_RANK        1
#define LARGE_NUMB_KEYS  1024000
#define LARGE_NUMB_MAPS  12800

#define OP_CREATE 100
#define OP_WRITE  200
#define OP_CLOSE  300
#define OP_DELETE 400
#define OP_OPEN   500
#define OP_READ   600

#define OBJ_GROUP 1
#define OBJ_DTYPE 2
#define OBJ_DSET  3
#define OBJ_MAP   4
#define OBJ_ATTR  5

typedef struct {
    int   numbOfGroups;
    int   dset_dim1;
    int   dset_dim2;
    int   attr_dim;
    int   numbOfMapEntries;
    char *server_ranks_str;
    char *fault_groups_str;
    char *fault_op_str;
    char *fault_obj_str;
    int   nFaults;
    char *daosObjClass;
    char *collMetadata;
    char *readOrWriteToKillServers;
} handler_t;

typedef struct {
    d_rank_t *daos_server_ranks;
    int      *fault_groups;
    int      *fault_ops;
} command_line_info_t;

/*
 * Global variables
 */
char                       pool[DAOS_PROP_LABEL_MAX_LEN + 1];
int                        mpi_rank;
static int                 mpi_size;
static int                *wdata, *rdata;
static int                *map_keys, *map_vals, *map_vals_out;
static int                *attr_write, *attr_read;
static hid_t               file_dspace, file_dspace_select, mem_space;
static hid_t               attr_space;
static handler_t           hand;
static command_line_info_t cl_info;
static int                 server_count = 0;

#define FAULT_INJECTION(I, J)                                                                                \
    if (hand.nFaults && I == cl_info.fault_groups[server_count] && J == cl_info.fault_ops[server_count]) {   \
        inject_fault(cl_info.daos_server_ranks[server_count]);                                               \
        server_count++;                                                                                      \
    }

/*
 * Function prototypes
 */
static int figure_out_op(const char *);

/*
 * Reusable private function to kill and exclude a certain server
 */
static void
inject_fault(d_rank_t which_server)
{
    char dmg_cmd[100];
    int  rc;

    if (MAINPROCESS) {
        /* Kill the server */
        snprintf(dmg_cmd, sizeof(dmg_cmd), "dmg system stop -i --ranks=%d --force", which_server);
        rc = system(dmg_cmd);
        if (rc != 0) {
            printf(" %s failed with rc %#x\n", dmg_cmd, rc);
            return;
        }

        /* Exclude the server from the pool */
        snprintf(dmg_cmd, sizeof(dmg_cmd), "dmg pool exclude -i --pool=%s --ranks=%d", pool, which_server);
        rc = system(dmg_cmd);
        if (rc != 0) {
            printf(" %s failed with rc %#x\n", dmg_cmd, rc);
            return;
        }

        fprintf(stdout, "\n\n\n\n        ========>>> Killed and excluded the server (rank %d)\n\n\n\n",
                which_server);
    }
}

static void
initialize_data()
{
    int i, j;

    /* Allocate the memory for the dataset write and read */
    wdata = (int *)malloc((size_t)((hand.dset_dim1 / mpi_size) * hand.dset_dim2) * sizeof(int));
    rdata = (int *)malloc((size_t)((hand.dset_dim1 / mpi_size) * hand.dset_dim2) * sizeof(int));

    /* Initialize the data for the dataset */
    for (i = 0; i < hand.dset_dim1 / mpi_size; i++)
        for (j = 0; j < hand.dset_dim2; j++)
            *(wdata + i * hand.dset_dim2 + j) = i + j;

    /* Allocate the memory for the map entries */
    map_keys     = (int *)malloc((size_t)hand.numbOfMapEntries * sizeof(int));
    map_vals     = (int *)malloc((size_t)hand.numbOfMapEntries * sizeof(int));
    map_vals_out = (int *)malloc((size_t)hand.numbOfMapEntries * sizeof(int));

    /* Generate random keys and values for the map */
    for (i = 0; i < hand.numbOfMapEntries; i++) {
        map_keys[i] = (rand() % (256 * 256 * 256 * 32 / hand.numbOfMapEntries)) * hand.numbOfMapEntries + i;
        map_vals[i] = rand();
    } /* end for */

    /* Allocate the memory for the attribute write and read */
    attr_write = (int *)malloc((size_t)hand.attr_dim * sizeof(int));
    attr_read  = (int *)malloc((size_t)hand.attr_dim * sizeof(int));

    /* Initialize the data for the attribute */
    for (i = 0; i < hand.attr_dim; i++)
        attr_write[i] = i;
}

static int
create_dataspace()
{
    hsize_t dimsf[DATA_RANK];                    /* dataset dimensions */
    hsize_t start[DATA_RANK];                    /* for hyperslab setting */
    hsize_t count[DATA_RANK], stride[DATA_RANK]; /* for hyperslab setting */
    hsize_t attr_dim[ATTR_RANK];

    dimsf[0] = (hsize_t)hand.dset_dim1;
    dimsf[1] = (hsize_t)hand.dset_dim2;
    if ((file_dspace = H5Screate_simple(DATA_RANK, dimsf, NULL)) < 0) {
        H5_FAILED();
        AT();
        printf("failed to create the data space\n");
        goto error;
    }

    if ((file_dspace_select = H5Scopy(file_dspace)) < 0) {
        H5_FAILED();
        AT();
        printf("failed to copy the file data space\n");
        goto error;
    }

    /* set up dimensions of the slab this process accesses */
    start[0]  = (hsize_t)mpi_rank * dimsf[0] / (hsize_t)mpi_size;
    start[1]  = 0;
    count[0]  = dimsf[0] / (hsize_t)mpi_size;
    count[1]  = dimsf[1];
    stride[0] = 1;
    stride[1] = 1;

    if (H5Sselect_hyperslab(file_dspace_select, H5S_SELECT_SET, start, stride, count, NULL) < 0) {
        H5_FAILED();
        AT();
        printf("failed to do hyperslab selection\n");
        goto error;
    }

    if ((mem_space = H5Screate_simple(DATA_RANK, count, NULL)) < 0) {
        H5_FAILED();
        AT();
        printf("failed to create memory space\n");
        goto error;
    }

    attr_dim[0] = (hsize_t)hand.attr_dim;
    if ((attr_space = H5Screate_simple(ATTR_RANK, attr_dim, NULL)) < 0) {
        H5_FAILED();
        AT();
        printf("failed to create memory space\n");
        goto error;
    }

    return 0;

error:
    return -1;
}

static int
close_ids()
{
    /* Free the memory buffers for the dataset */
    if (wdata)
        free(wdata);
    if (rdata)
        free(rdata);

    /* Free the memory buffers for the map */
    if (map_keys)
        free(map_keys);
    if (map_vals)
        free(map_vals);
    if (map_vals_out)
        free(map_vals_out);

    /* Free the memory buffers for the attribute */
    if (attr_write)
        free(attr_write);
    if (attr_read)
        free(attr_read);

    if (H5Sclose(mem_space) < 0) {
        H5_FAILED();
        AT();
        printf("failed to close the memory data space\n");
        goto error;
    }

    if (H5Sclose(file_dspace) < 0) {
        H5_FAILED();
        AT();
        printf("failed to close the file space\n");
        goto error;
    }

    if (H5Sclose(file_dspace_select) < 0) {
        H5_FAILED();
        AT();
        printf("failed to close the file space selection\n");
        goto error;
    }

    if (H5Sclose(attr_space) < 0) {
        H5_FAILED();
        AT();
        printf("failed to close the attribute data space \n");
        goto error;
    }

    /* Free the strings from the command-line options */
    if (hand.daosObjClass)
        free(hand.daosObjClass);

    if (hand.server_ranks_str)
        free(hand.server_ranks_str);

    /* Free the information derived from the command line options */
    if (cl_info.daos_server_ranks)
        free(cl_info.daos_server_ranks);

    if (cl_info.fault_groups)
        free(cl_info.fault_groups);

    return 0;

error:
    return 1;
}

static htri_t
create_objects(hid_t file)
{
    hid_t   group, group_previous, dataset1, dataset2, datatype;
    hid_t   attr1_id, attr2_id, map_id;
    char    group_name[NAME_LENGTH];
    char    group_previous_name[NAME_LENGTH];
    char    dset1_name[NAME_LENGTH], dset2_name[NAME_LENGTH];
    char    dtype_name[NAME_LENGTH];
    char    attr1_name[NAME_LENGTH], attr2_name[NAME_LENGTH];
    char    map_name[NAME_LENGTH];
    hsize_t array_dtype_dims[ARRAY_DTYPE_RANK];
    int     i, j;

    if (MAINPROCESS) {
        fprintf(stdout, "========================\n");
        fprintf(stdout, "Started to create groups with named datatypes, datasets, maps, attributes\n\n");
    }

    for (i = 0; i < hand.numbOfGroups; i++) {
        if (MAINPROCESS)
            fprintf(stdout, "\r %d/%d in creating groups", i + 1, hand.numbOfGroups);
        FAULT_INJECTION(i, OP_CREATE + OBJ_GROUP);

        /* Create a group */
        snprintf(group_name, NAME_LENGTH, "group_%d", i);
        if ((group = H5Gcreate2(file, group_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            AT();
            printf("failed to create the group: %s\n", group_name);
            goto error;
        }

        /* Dimension for the array datatype */
        for (j = 0; j < ARRAY_DTYPE_RANK; j++)
            array_dtype_dims[j] = (hsize_t)(i + 1);

        /* Create an array datatype */
        if ((datatype = H5Tarray_create2(H5T_STD_I32LE, ARRAY_DTYPE_RANK, array_dtype_dims)) < 0) {
            H5_FAILED();
            AT();
            printf("failed to create the array datatype\n");
            goto error;
        }

        FAULT_INJECTION(i, OP_CREATE + OBJ_DTYPE);

        /* Commit the datatype into the group */
        snprintf(dtype_name, NAME_LENGTH, "datatype_%d", i);
        if (H5Tcommit2(group, dtype_name, datatype, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to commit the array datatype\n");
            goto error;
        }

        FAULT_INJECTION(i, OP_CLOSE + OBJ_DTYPE);

        /* Close the array datatype */
        if (H5Tclose(datatype) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close the datatype: %s\n", dtype_name);
            goto error;
        }

        FAULT_INJECTION(i, OP_CREATE + OBJ_DSET);

        /* Create the first dataset */
        snprintf(dset1_name, NAME_LENGTH, "dset1_%d", i);
        if ((dataset1 = H5Dcreate2(group, dset1_name, H5T_NATIVE_INT, file_dspace, H5P_DEFAULT, H5P_DEFAULT,
                                   H5P_DEFAULT)) < 0) {
            H5_FAILED();
            AT();
            printf("failed to create the first dataset: %s\n", dset1_name);
            goto error;
        }

        /* Create the second dataset */
        snprintf(dset2_name, NAME_LENGTH, "dset2_%d", i);
        if ((dataset2 = H5Dcreate2(group, dset2_name, H5T_NATIVE_INT, file_dspace, H5P_DEFAULT, H5P_DEFAULT,
                                   H5P_DEFAULT)) < 0) {
            H5_FAILED();
            AT();
            printf("failed to create the second dataset: %s\n", dset2_name);
            goto error;
        }

        FAULT_INJECTION(i, OP_WRITE + OBJ_DSET);

        /* Write data to the first dataset */
        if (H5Dwrite(dataset1, H5T_NATIVE_INT, mem_space, file_dspace_select, H5P_DEFAULT, wdata) < 0) {
            H5_FAILED();
            AT();
            printf("failed to write data to the first dataset: %s\n", dset1_name);
            goto error;
        }

        /* Write data to the second dataset */
        if (H5Dwrite(dataset2, H5T_NATIVE_INT, mem_space, file_dspace_select, H5P_DEFAULT, wdata) < 0) {
            H5_FAILED();
            AT();
            printf("failed to write data to the second dataset: %s\n", dset2_name);
            goto error;
        }

        FAULT_INJECTION(i, OP_CLOSE + OBJ_DSET);

        if (H5Dclose(dataset1) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close the first dataset: %s\n", dset1_name);
            goto error;
        }

        if (H5Dclose(dataset2) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close the second dataset: %s\n", dset2_name);
            goto error;
        }

        FAULT_INJECTION(i, OP_CREATE + OBJ_MAP);

        /* Create a map object */
        snprintf(map_name, NAME_LENGTH, "map_%d", i);
        if ((map_id = H5Mcreate(group, map_name, H5T_NATIVE_INT, H5T_NATIVE_INT, H5P_DEFAULT, H5P_DEFAULT,
                                H5P_DEFAULT)) < 0) {
            H5_FAILED();
            AT();
            printf("    couldn't create map: %s\n", map_name);
            goto error;
        } /* end if */

        FAULT_INJECTION(i, OP_WRITE + OBJ_MAP);

        for (j = 0; j < hand.numbOfMapEntries; j++) {
            if (H5Mput(map_id, H5T_NATIVE_INT, &map_keys[j], H5T_NATIVE_INT, &map_vals[j], H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to set key-value pair\n");
                goto error;
            } /* end if */
        }

        FAULT_INJECTION(i, OP_CLOSE + OBJ_MAP);

        if (H5Mclose(map_id) < 0) {
            H5_FAILED();
            AT();
            printf("    couldn't close map: %s\n", map_name);
            goto error;
        } /* end if */

        FAULT_INJECTION(i, OP_CREATE + OBJ_ATTR);

        /* Create the first attribute for the group */
        snprintf(attr1_name, NAME_LENGTH, "attr1_%d", i);
        if ((attr1_id = H5Acreate2(group, attr1_name, H5T_NATIVE_INT, attr_space, H5P_DEFAULT, H5P_DEFAULT)) <
            0) {
            H5_FAILED();
            AT();
            printf("failed to create the first attribute: %s\n", attr1_name);
            goto error;
        }

        /* Create the second attribute for the group */
        snprintf(attr2_name, NAME_LENGTH, "attr2_%d", i);
        if ((attr2_id = H5Acreate2(group, attr2_name, H5T_NATIVE_INT, attr_space, H5P_DEFAULT, H5P_DEFAULT)) <
            0) {
            H5_FAILED();
            AT();
            printf("failed to create the first attribute: %s\n", attr1_name);
            goto error;
        }

        FAULT_INJECTION(i, OP_WRITE + OBJ_ATTR);

        if (H5Awrite(attr1_id, H5T_NATIVE_INT, attr_write) < 0) {
            H5_FAILED();
            AT();
            printf("failed to write data to the first attribute: %s\n", attr1_name);
            goto error;
        }

        if (H5Awrite(attr2_id, H5T_NATIVE_INT, attr_write) < 0) {
            H5_FAILED();
            AT();
            printf("failed to write data to the second attribute: %s\n", attr2_name);
            goto error;
        }

        FAULT_INJECTION(i, OP_CLOSE + OBJ_ATTR);

        if (H5Aclose(attr1_id) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close the first attribute\n");
            goto error;
        }

        if (H5Aclose(attr2_id) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close the second attribute\n");
            goto error;
        }

        FAULT_INJECTION(i, OP_CLOSE + OBJ_GROUP);

        if (H5Gclose(group) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close the group: %s\n", group_name);
            goto error;
        }

        /* Delete a dataset and an attribute from the previous iteration */
        if (i > 0) {
            /* Open the group in the previous iteration*/
            snprintf(group_previous_name, NAME_LENGTH, "group_%d", i - 1);
            if ((group_previous = H5Gopen2(file, group_previous_name, H5P_DEFAULT)) < 0) {
                H5_FAILED();
                AT();
                printf("failed to open the group in the previous iteration: %s\n", group_previous_name);
                goto error;
            }

            FAULT_INJECTION(i, OP_DELETE + OBJ_DSET);

            /* Delete the second dataset in the previous iteration */
            snprintf(dset2_name, NAME_LENGTH, "dset2_%d", i - 1);
            if (H5Ldelete(group_previous, dset2_name, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to delete the second dataset in the previous iteration: %s\n", dset2_name);
                goto error;
            }

            FAULT_INJECTION(i, OP_DELETE + OBJ_ATTR);

            /* Delete the second attribute in the previous iteration */
            snprintf(attr2_name, NAME_LENGTH, "attr2_%d", i - 1);
            if (H5Adelete(group_previous, attr2_name) < 0) {
                H5_FAILED();
                AT();
                printf("failed to delete the second attribute in the previous iteration: %s\n", attr2_name);
                goto error;
            }

            if (H5Gclose(group_previous) < 0) {
                H5_FAILED();
                AT();
                printf("failed to close the group: %s\n", group_name);
                goto error;
            }
        }
    }

    if (MAINPROCESS)
        fprintf(stdout, "\n\nFinished creating groups with named datatypes, datasets, maps, attributes\n");

    return 0;

error:

    return -1;
}

static int
open_objects(hid_t file)
{
    hid_t   group, dataset, datatype = -1;
    hid_t   attr1_id, map_id; /* handles */
    char    group_name[NAME_LENGTH];
    char    dset_name[NAME_LENGTH];
    char    dtype_name[NAME_LENGTH];
    char    attr1_name[NAME_LENGTH];
    char    map_name[NAME_LENGTH];
    hsize_t array_dtype_dims[ARRAY_DTYPE_RANK];
    int     i, j, k;

    if (MAINPROCESS) {
        fprintf(stdout, "========================\n");
        fprintf(stdout, "Started to open groups with named datatypes, datasets, maps, attributes\n\n");
    }

    for (k = 0; k < hand.numbOfGroups; k++) {
        if (MAINPROCESS)
            fprintf(stdout, "\r %d/%d in reading groups", k + 1, hand.numbOfGroups);

        FAULT_INJECTION(k, OP_OPEN + OBJ_GROUP);

        /* open a group */
        snprintf(group_name, NAME_LENGTH, "group_%d", k);
        if ((group = H5Gopen2(file, group_name, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            AT();
            printf("failed to open the group: %s\n", group_name);
            goto error;
        }

        FAULT_INJECTION(k, OP_OPEN + OBJ_DTYPE);

        /* Open the committed datatype in the group and make sure it's correct */
        snprintf(dtype_name, NAME_LENGTH, "datatype_%d", k);
        if ((datatype = H5Topen2(group, dtype_name, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            AT();
            printf("failed to open the array datatype: %s\n", dtype_name);
            goto error;
        }

        if (H5T_ARRAY != H5Tget_class(datatype)) {
            H5_FAILED();
            AT();
            printf("datatype is not an array type: %s\n", dtype_name);
            goto error;
        }

        FAULT_INJECTION(k, OP_READ + OBJ_DTYPE);

        if (ARRAY_DTYPE_RANK != H5Tget_array_dims2(datatype, array_dtype_dims)) {
            H5_FAILED();
            AT();
            printf("the dimension rank of the array datatype is not correct: %s\n", dtype_name);
            goto error;
        }

        /* Dimension for the array datatype */
        for (j = 0; j < ARRAY_DTYPE_RANK; j++) {
            if (array_dtype_dims[j] != (unsigned)(k + 1)) {
                H5_FAILED();
                AT();
                printf("wrong dimension of the array datatype: array_dtype_dims[%d]=%llu\n", j,
                       (unsigned long long)array_dtype_dims[j]);
                goto error;
            }
        }

        FAULT_INJECTION(k, OP_CLOSE + OBJ_DTYPE);

        /* Close the array datatype */
        if (H5Tclose(datatype) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close the datatype (%ld): %s\n", datatype, dtype_name);
            goto error;
        }

        FAULT_INJECTION(k, OP_OPEN + OBJ_DSET);

        snprintf(dset_name, NAME_LENGTH, "dset1_%d", k);
        if ((dataset = H5Dopen2(group, dset_name, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            AT();
            printf("failed to open the dataset: %s\n", dset_name);
            goto error;
        }

        FAULT_INJECTION(k, OP_READ + OBJ_DSET);

        if (H5Dread(dataset, H5T_NATIVE_INT, mem_space, file_dspace_select, H5P_DEFAULT, rdata) < 0) {
            H5_FAILED();
            AT();
            printf("failed to read the dataset: %s\n", dset_name);
            goto error;
        }

        for (i = 0; i < hand.dset_dim1 / mpi_size; i++)
            for (j = 0; j < hand.dset_dim2; j++)
                if (*(rdata + i * hand.dset_dim2 + j) != *(wdata + i * hand.dset_dim2 + j)) {
                    H5_FAILED();
                    AT();
                    printf("wrong data: rdata[%d][%d]=%d, wdata[%d][%d]=%d\n", j, i,
                           *(rdata + i * hand.dset_dim2 + j), j, i, *(wdata + i * hand.dset_dim2 + j));
                    goto error;
                }

        FAULT_INJECTION(k, OP_CLOSE + OBJ_DSET);

        if (H5Dclose(dataset) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close the dataset: %s\n", dset_name);
            goto error;
        }

        FAULT_INJECTION(k, OP_OPEN + OBJ_MAP);

        /* Open the map and make sure the key-value pair is correct */
        snprintf(map_name, NAME_LENGTH, "map_%d", k);
        if ((map_id = H5Mopen(group, map_name, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            AT();
            printf("    couldn't open the map: %s\n", map_name);
            goto error;
        } /* end if */

        FAULT_INJECTION(k, OP_READ + OBJ_MAP);

        for (i = 0; i < hand.numbOfMapEntries; i++) {
            if (H5Mget(map_id, H5T_NATIVE_INT, &map_keys[i], H5T_NATIVE_INT, &map_vals_out[i], H5P_DEFAULT) <
                0) {
                H5_FAILED();
                AT();
                printf("failed to get %dth key-value pair for the map: %s\n", i, map_name);
                goto error;
            } /* end if */

            if (map_vals_out[i] != map_vals[i]) {
                H5_FAILED();
                AT();
                printf("incorrect %dth value returned %d, key is %d\n", i, map_vals_out[i], map_keys[i]);
                goto error;
            } /* end if */
        }

        FAULT_INJECTION(k, OP_CLOSE + OBJ_MAP);

        if (H5Mclose(map_id) < 0) {
            H5_FAILED();
            AT();
            printf("    couldn't close the map: %s\n", map_name);
            goto error;
        } /* end if */

        FAULT_INJECTION(k, OP_OPEN + OBJ_ATTR);

        /* Open the first attribute for the group and verify the data*/
        snprintf(attr1_name, NAME_LENGTH, "attr1_%d", k);
        if ((attr1_id = H5Aopen(group, attr1_name, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            AT();
            printf("failed to open the first attribute: %s\n", attr1_name);
            goto error;
        }

        FAULT_INJECTION(k, OP_READ + OBJ_ATTR);

        if (H5Aread(attr1_id, H5T_NATIVE_INT, attr_read) < 0) {
            H5_FAILED();
            AT();
            printf("failed to write data to the first attribute: %s\n", attr1_name);
            goto error;
        }

        for (i = 0; i < hand.attr_dim; i++) {
            if (attr_read[i] != attr_write[i]) {
                H5_FAILED();
                AT();
                printf("wrong attribute data: attr_read[%d]=%d, attr_write[%d]=%d\n", i, attr_read[i], i,
                       attr_write[i]);
                goto error;
            }
        }

        FAULT_INJECTION(k, OP_CLOSE + OBJ_ATTR);

        if (H5Aclose(attr1_id) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close the first attribute\n");
            goto error;
        }

        FAULT_INJECTION(k, OP_CLOSE + OBJ_GROUP);

        if (H5Gclose(group) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close the group: %s\n", dset_name);
            goto error;
        }
    }

    if (MAINPROCESS)
        fprintf(stdout, "\n\nFinished opening groups with named datatypes, datasets, maps, attributes\n");

    return 0;

error:
    return -1;
}

/*
 *  * Show command usage
 */
static void
usage(void)
{
    printf("    [-h] [-a --dimOfAttr] [-d --dimsDset] [-e --mapEntries] [-i --nGroups] [-j --faultGroups]\n");
    printf("    [-k --objects] [-l --faultOps] [-m --collMetadata] \n");
    printf("    [-n --nFaultInjects] [-r --daosObjClass] [-s --daosServerRanks]\n");

    printf("    [-h --help]: this help page\n");
    printf("    [-a --dimOfAttr]: the single dimension of the attributes\n");
    printf("    [-d --dimsDset]: this 2D dimensions of the datasets, e.g. 16x8\n");
    printf("    [-e --mapEntries]: the number of the map entries\n");
    printf("    [-i --nGroups]: number of groups with datatypes, datasets, attributes, and maps in them\n");
    printf("    [-j --faultGroups]: in which iteration (group) to kill and exclude a server (starting from "
           "0)\n");
    printf("    [-k --objects]: \n");
    printf("\t1   - values include group, dtype, dset, map, attr\n");
    printf("    [-l --faultOps]: where the fault injection should happen (used with the -k option)\n");
    printf("\t1   - values include create, write, delete, open, read, close\n");
    printf("\t1   - objects for create: group, dtype, map, dset, attr\n");
    printf("\t1   - objects for write: map, dset, attr\n");
    printf("\t1   - objects for delete: dset, attr\n");
    printf("\t1   - objects for open: group, dtype, map, dset, attr\n");
    printf("\t1   - objects for read: map, dset, attr\n");
    printf("\t1   - objects for close: group, dtype, map, dset, attr\n");
    printf("    [-m --collMetadata]: mode of parallel - independent or collective (only collective is "
           "supported now)\n");
    printf("    [-n --nFaultInjects]: number of servers to kill and exclude\n");
    printf("    [-r --daosObjClass]: object class, e.g. S1, S2, RP_2G1, RP_3G1\n");
    printf("    [-s --daosServerRanks]: the rank of the servers to be killed (at most 2 servers, separated "
           "by comma), e.g. 0,2\n");
    printf("\n");
}

static void
parse_command_line(int argc, char *argv[])
{
    int           opt;
    struct option long_options[] = {{"dimOfAttr=", required_argument, NULL, 'a'},
                                    {"dimsDset=", required_argument, NULL, 'd'},
                                    {"mapEntries=", required_argument, NULL, 'e'},
                                    {"help", no_argument, NULL, 'h'},
                                    {"nGroups=", required_argument, NULL, 'i'},
                                    {"faultGroups=", required_argument, NULL, 'j'},
                                    {"objects=", required_argument, NULL, 'k'},
                                    {"faultOps=", required_argument, NULL, 'l'},
                                    {"collMetadata=", required_argument, NULL, 'm'},
                                    {"nFaultInjects=", required_argument, NULL, 'n'},
                                    {"daosServerRanks=", required_argument, NULL, 's'},
                                    {"daosObjClass=", required_argument, NULL, 'r'},
                                    {"readOrWriteToKillServers=", required_argument, NULL, 'w'},
                                    {NULL, 0, NULL, 0}};

    /* Initialize the command line options */
    hand.numbOfGroups             = 100;
    hand.dset_dim1                = 100;
    hand.dset_dim2                = 100;
    hand.attr_dim                 = 100;
    hand.numbOfMapEntries         = 10;
    hand.collMetadata             = strdup("collective");
    hand.nFaults                  = 0;
    hand.daosObjClass             = strdup("S1");
    hand.readOrWriteToKillServers = strdup("read");

    if (MAINPROCESS)
        fprintf(stdout, "\n\n");

    while ((opt = getopt_long(argc, argv, "a:d:e:hi:j:k:l:m:n:r:s:w:", long_options, NULL)) != -1) {
        switch (opt) {
            case 'a':
                /* The single dimension of the attribute */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "dimension of attribute: 				%s\n", optarg);
                    hand.attr_dim = atoi(optarg);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'd':
                /* The dimensions of the dataset */
                if (optarg) {
                    char *dims_str, *dim1_str, *dim2_str;
                    if (MAINPROCESS)
                        fprintf(stdout, "dimensions of dataset: 				%s\n", optarg);
                    dims_str       = strdup(optarg);
                    dim1_str       = strtok(dims_str, "x");
                    dim2_str       = strtok(NULL, "x");
                    hand.dset_dim1 = atoi(dim1_str);
                    hand.dset_dim2 = atoi(dim2_str);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'e':
                /* Number of map entries */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "number of map entries: 				%s\n", optarg);
                    hand.numbOfMapEntries = atoi(optarg);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'h':
                if (MAINPROCESS) {
                    fprintf(stdout, "Help page:\n");
                    usage();
                }

                MPI_Finalize();
                exit(0);

                break;
            case 'i':
                /* Number of iteration for groups, datasets, datatypes, attributes, and maps */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "number of groups: 					%s\n", optarg);
                    hand.numbOfGroups = atoi(optarg);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'j':
                /* Which group to kill and exclude a server */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "which iteration (group) to kill and exclude servers: 	%s\n",
                                optarg);

                    hand.fault_groups_str = strdup(optarg);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'k':
                /* What object for the operation */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "where to kill and exclude servers: 			%s\n", optarg);

                    hand.fault_obj_str = strdup(optarg);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'l':
                /* What operation (create, write, etc) on the objects to kill and exclude a server */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "what operation on the object to do fault injections: 	%s\n",
                                optarg);

                    hand.fault_op_str = strdup(optarg);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'm':
                /* Mode of parallel IO: collective or independent */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "whether to use collective mode (collective is the default): %s\n",
                                optarg);
                    if (hand.collMetadata)
                        free(hand.collMetadata);
                    hand.collMetadata = strdup(optarg);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'n':
                /* Number of servers to be killed and excluded */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "number of servers to be killed: 			%s\n", optarg);
                    hand.nFaults = atoi(optarg);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'r':
                /* Replicated object class */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "replicated object class: 				%s\n", optarg);
                    if (hand.daosObjClass)
                        free(hand.daosObjClass);
                    hand.daosObjClass = strdup(optarg);
                }
                else
                    printf("optarg is null\n");
                break;
            case 's':
                /* The set of the server ranks to be killed */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "list of server ranks for fault injection: 		%s\n", optarg);

                    hand.server_ranks_str = strdup(optarg);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'w':
                /* During read or write to kill the servers */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "during read or write to kill the servers: 		%s\n", optarg);
                    if (hand.readOrWriteToKillServers)
                        free(hand.readOrWriteToKillServers);
                    hand.readOrWriteToKillServers = strdup(optarg);
                }
                else
                    printf("optarg is null\n");
                break;
            case ':':
                printf("option needs a value\n");
                break;
            case '?':
                printf("unknown option: %c\n", optopt);
                break;
        }
    }

    /* optind is for the extra arguments which are not parsed */
    for (; optind < argc; optind++) {
        printf("extra arguments not parsed: %s\n", argv[optind]);
    }
}

static int
get_command_line_info(hid_t file_id)
{
    char *opt_str_cp = NULL;
    char *token_str  = NULL;
    int   i;

    if (hand.nFaults) {
        /* Figure out the set of the server ranks for fault injection */
        if (hand.server_ranks_str)
            opt_str_cp = strdup(hand.server_ranks_str);

        cl_info.daos_server_ranks = malloc((size_t)hand.nFaults * sizeof(d_rank_t));

        if (opt_str_cp) {
            int rank_number;

            token_str = strtok(opt_str_cp, ",");

            if ((rank_number = atoi(token_str)) < 0)
                return -1;
            cl_info.daos_server_ranks[0] = (d_rank_t)rank_number;

            i = 1;
            while (token_str) {
                token_str = strtok(NULL, ",");
                if (token_str && i < hand.nFaults) {
                    if ((rank_number = atoi(token_str)) < 0)
                        return -1;
                    cl_info.daos_server_ranks[i] = (d_rank_t)rank_number;
                }
                i++;
            }

            free(opt_str_cp);
        }
        else { /* If no option is passed in, use the default: the list starts from the highest server rank and
                  descend for the number of fault injections */
            daos_handle_t    poh;
            daos_pool_info_t info;

            /* Get the pool object handle */
            H5daos_get_poh(file_id, &poh);

            /* Query the pool information */
            daos_pool_query(poh, NULL, &info, NULL, NULL);

            for (i = 0; i < hand.nFaults; i++)
                cl_info.daos_server_ranks[i] = (d_rank_t)(info.pi_nnodes - 1 - (uint32_t)i);
        }

        /* Figure out the list of iterations for fault injection */
        if (hand.fault_groups_str)
            opt_str_cp = strdup(hand.fault_groups_str);

        cl_info.fault_groups = (int *)malloc((size_t)hand.nFaults * sizeof(int));

        /* Default value is the first group */
        for (i = 0; i < hand.nFaults; i++)
            cl_info.fault_groups[i] = 0;

        if (opt_str_cp) {
            token_str = strtok(opt_str_cp, ",");

            if (token_str)
                cl_info.fault_groups[0] = atoi(token_str);

            i = 1;
            while (token_str) {
                token_str = strtok(NULL, ",");
                if (token_str && i < hand.nFaults)
                    cl_info.fault_groups[i] = atoi(token_str);
                i++;
            }

            free(opt_str_cp);
        }

        /* Figure out the list of object operations for fault injection */
        if (hand.fault_op_str)
            opt_str_cp = strdup(hand.fault_op_str);

        cl_info.fault_ops = (int *)malloc((size_t)hand.nFaults * sizeof(int));

        /* Default value is the first group */
        for (i = 0; i < hand.nFaults; i++)
            cl_info.fault_ops[i] = 100;

        if (opt_str_cp) {
            token_str = strtok(opt_str_cp, ",");

            if (token_str)
                cl_info.fault_ops[0] = figure_out_op(token_str);

            i = 1;
            while (token_str) {
                token_str = strtok(NULL, ",");
                if (token_str && i < hand.nFaults)
                    cl_info.fault_ops[i] = figure_out_op(token_str);
                i++;
            }

            free(opt_str_cp);
        }

        /* Figure out the list of objects and add the value to the list of the operations for simplicity */
        if (hand.fault_obj_str)
            opt_str_cp = strdup(hand.fault_obj_str);

        if (opt_str_cp) {
            token_str = strtok(opt_str_cp, ",");

            if (token_str)
                cl_info.fault_ops[0] += figure_out_op(token_str);

            i = 1;
            while (token_str) {
                token_str = strtok(NULL, ",");
                if (token_str && i < hand.nFaults)
                    cl_info.fault_ops[i] += figure_out_op(token_str);
                i++;
            }

            free(opt_str_cp);
        }
    }

    return 0;
}

static int
figure_out_op(const char *str)
{
    if (!strcmp(str, "create"))
        return OP_CREATE;
    else if (!strcmp(str, "write"))
        return OP_WRITE;
    else if (!strcmp(str, "close"))
        return OP_CLOSE;
    else if (!strcmp(str, "delete"))
        return OP_DELETE;
    else if (!strcmp(str, "open"))
        return OP_OPEN;
    else if (!strcmp(str, "read"))
        return OP_READ;
    else if (!strcmp(str, "group"))
        return OBJ_GROUP;
    else if (!strcmp(str, "dtype"))
        return OBJ_DTYPE;
    else if (!strcmp(str, "dset"))
        return OBJ_DSET;
    else if (!strcmp(str, "map"))
        return OBJ_MAP;
    else if (!strcmp(str, "attr"))
        return OBJ_ATTR;
    else
        return 0;
}

/*
 * main function
 */
int
main(int argc, char **argv)
{
    char  filename[NAME_LENGTH];
    hid_t fapl_id = -1, file_id = -1;
    char *pool_string = NULL;
    int   nerrors     = 0;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);

    /* Ensure that connector and HDF5 library are initialized */
    H5open();

    parse_command_line(argc, argv);

    if (hand.dset_dim1 % mpi_size) {
        printf("DIM1(%d) must be multiples of processes (%d)\n", hand.dset_dim1, mpi_size);
        nerrors++;
        goto error;
    }

    snprintf(filename, NAME_LENGTH, "%s", FILENAME);

    if ((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0) {
        nerrors++;
        goto error;
    }

    if (H5Pset_fapl_mpio(fapl_id, MPI_COMM_WORLD, MPI_INFO_NULL) < 0) {
        nerrors++;
        goto error;
    }

    if (H5Pset_all_coll_metadata_ops(fapl_id, TRUE) < 0) {
        nerrors++;
        goto error;
    }

    if (H5Pset_coll_metadata_write(fapl_id, TRUE) < 0) {
        nerrors++;
        goto error;
    }

    if (H5daos_set_object_class(fapl_id, hand.daosObjClass) < 0) {
        nerrors++;
        goto error;
    }

    if ((file_id = H5Fcreate(FILENAME, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0) {
        nerrors++;
        goto error;
    }

    if (NULL != (pool_string = getenv("DAOS_POOL"))) {
        strcpy(pool, pool_string);
    }
    else {
        /* Try to retrieve pool UUID from the file */
        if (H5daos_get_pool(file_id, pool) < 0) {
            printf("Can't retrieve file's pool UUID\n\n");
            goto error;
        }
    }

    if (MAINPROCESS) {
        fprintf(stdout, "Test parameters:\n\n");
        fprintf(stdout, "  - Pool: %s\n", pool);
        fprintf(stdout, "  - Test File name: %s\n", filename);
        fprintf(stdout, "\n\n");
    }

    /* Retrieve some information from the command-line options */
    if (get_command_line_info(file_id) < 0) {
        nerrors++;
        goto error;
    }

    initialize_data();

    if (create_dataspace() < 0) {
        nerrors++;
        goto error;
    }

    /* Create groups with datasets, named datatypes, maps, and attributes in them */
    if (create_objects(file_id) < 0) {
        nerrors++;
        goto error;
    }

    /* Open groups with datasets, named datatypes, maps, and attributes in them and verify the data */
    if (open_objects(file_id) < 0) {
        nerrors++;
        goto error;
    }

    if (close_ids() < 0) {
        nerrors++;
        goto error;
    }

    if (H5Pclose(fapl_id) < 0) {
        nerrors++;
        goto error;
    }

    if (H5Fclose(file_id) < 0) {
        nerrors++;
        goto error;
    }

    if (nerrors)
        goto error;

    if (MAINPROCESS) {
        puts("========================\n");
        puts("\n\nAll tests passed");
    }

    MPI_Finalize();

    return 0;

error:
    if (MAINPROCESS)
        printf("*** %d TEST%s FAILED ***\n", nerrors, (!nerrors || nerrors > 1) ? "S" : "");

    MPI_Finalize();

    return 1;
} /* end main() */
