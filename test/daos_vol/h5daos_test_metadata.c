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
 * Purpose: Performance benchmark for metadata
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <hdf5.h>

#include "daos_vol_public.h"
#include "h5daos_test.h"

/*
 * Definitions
 */
#define TRUE                    1
#define FALSE                   0

#define FILENAME                "h5daos_test_mdata.h5"
#define NAME_LENGTH     	256
#define DSET_RANK		1
#define DSET_DIM		16

/* Struct for command-line options */
typedef struct {
    int numbOfTrees;
    int depthOfTree;
    int numbOfBranches;
    int numbOfObjs;
    hbool_t uniqueGroupPerRank;
    hbool_t runMPIIO;
    char *daosObjClass;
    char *collMetadata;
    char *fileName;
} handler_t;

/* List of object operations */
typedef enum {
  GROUP_CREATE_NUM 	= 0,
  GROUP_INFO_NUM 	= 1,
  GROUP_REMOVE_NUM 	= 2,
  DSET_CREATE_NUM 	= 3,
  DSET_READ_NUM 	= 4,
  DSET_REMOVE_NUM 	= 5,
  ATTR_CREATE_NUM 	= 6,
  ATTR_REMOVE_NUM 	= 7,
  DTYPE_COMMIT_NUM      = 8,
  MAP_CREATE_NUM 	= 9,
  MAP_REMOVE_NUM 	= 10,
  ENTRY_NUM
} test_num_t;

/* List of file operations */
typedef enum {
  FILE_CREATE_NUM 	= 0,
  FILE_REMOVE_NUM 	= 1,
  FILE_ENTRY_NUM
} file_num_t;

/* Global variables */
int    mpi_rank;
int    mpi_size;
static handler_t hand;
double *op_time[ENTRY_NUM];
double max_time[ENTRY_NUM];
double min_time[ENTRY_NUM];
double *file_op_time[FILE_ENTRY_NUM];
unsigned tree_order;

/* Show command usage */
static void
usage(void)
{
    printf("    [-h] [-a] [-b] [-i] [-I] [-m] [-o] [-r] [-u] [-z]\n\n"); 

    printf("    [-h]: this help page\n");
    printf("    [-a]: indicate to run H5MPIIO (otherwise H5VOL)\n");
    printf("    [-b]: the number of branches per tree node\n");
    printf("    [-i]: the number of trees (iterations)\n");
    printf("    [-I]: the number of objects (groups, datasets, attributes, and maps) per tree node\n");
    printf("    [-m]: mode of parallel - indepedent or collective (collective is the default)\n");
    printf("    [-o]: File name without .h5 extension (add the prefix 'daos:' for H5MPIIO)\n");
    printf("    [-r]: replicated object class, e.g. S1, S2, RP_2G1, RP_3G1\n");
    printf("    [-u]: unique group per rank where objects will be located under\n");
    printf("    [-z]: the number of levels (depth) for the tree (the tree root is at level 0) \n");
    printf("\n");
}

static int
parse_command_line(int argc, char *argv[])
{
    int opt;

    /* Initialize the command line options */
    hand.numbOfTrees = 1;
    hand.depthOfTree = 1;
    hand.numbOfBranches = 1;
    hand.numbOfObjs = 1;
    hand.uniqueGroupPerRank = FALSE;
    hand.runMPIIO = FALSE;
    hand.daosObjClass = strdup("S1");
    hand.collMetadata = strdup("collective");
    hand.fileName = strdup(FILENAME);

    while((opt = getopt(argc, argv, "ab:hi:I:m:o:r:uz:")) != -1) {
        switch(opt) {
            case 'a':
                /* Flag to indicate running H5MPIIO */
                if(MAINPROCESS)
                    fprintf(stdout, "run H5MPIIO:	 					TRUE\n");
                hand.runMPIIO = TRUE;
                break;
            case 'b':
                /* The number of branches for each group */
                if(optarg) {
                    if(MAINPROCESS)
                        fprintf(stdout, "branches of hierarchical tree: 				%s\n", optarg);
                    hand.numbOfBranches = atoi(optarg);
                } else
                    printf("optarg is null\n");
                break;
            case 'h': 
                if(MAINPROCESS) {
                    fprintf(stdout, "Help page:\n");
                    usage();
                }

                MPI_Finalize();
                exit(0);

                break;
            case 'i':
                /* The number of iterations (trees) for the test */
                if(optarg) {
                    if(MAINPROCESS)
                        fprintf(stdout, "number of iterations (trees): 				%s\n", optarg);
                    hand.numbOfTrees = atoi(optarg);
                } else
                    printf("optarg is null\n");
                break;
            case 'I':
                /* The number of objects (groups, datasets, attributes, and maps) per node */
                if(optarg) {
                    if(MAINPROCESS)
                        fprintf(stdout, "number of objects per tree node: 			%s\n", optarg);
                    hand.numbOfObjs = atoi(optarg);
                } else
                    printf("optarg is null\n");
                break;
            case 'm':
                /* Mode of parallel IO: collective or independent */
                if(optarg) { 
                    if(MAINPROCESS)
                        fprintf(stdout, "whether to use collective mode (collective is the default): %s\n", optarg);
                    if(hand.collMetadata)
                        free(hand.collMetadata);
                    hand.collMetadata = strdup(optarg);
                } else
                    printf("optarg is null\n");
                break;
            case 'o':
                /* File name without .h5 extension (add the prefix 'daos:' for H5MPIIO) */
                if(optarg) { 
                    if(MAINPROCESS)
                        fprintf(stdout, "HDF5 file name: 					%s\n", optarg);
                    if(hand.fileName)
                        free(hand.fileName);
                    hand.fileName = strdup(optarg);
                } else
                    printf("optarg is null\n");
                break;
            case 'r':
                /* Replicated object class */
                if(optarg) {
                    if(MAINPROCESS)
                        fprintf(stdout, "replicated object class: 				%s\n", optarg);
                    if(hand.daosObjClass)
                        free(hand.daosObjClass);
                    hand.daosObjClass = strdup(optarg);
                } else
                    printf("optarg is null\n");
                break;
            case 'u':
                /* Unique group per rank where objects will be located under */
                if(MAINPROCESS)
                    fprintf(stdout, "unique group per rank: 					TRUE\n");
                hand.uniqueGroupPerRank = TRUE;
                break;
            case 'z':
                /* The number of levels (depth) for the tree */
                if(optarg) {
                    if(MAINPROCESS)
                        fprintf(stdout, "depth of hierarchical groups (tree): 			%s\n", optarg);
                    hand.depthOfTree = atoi(optarg);
                } else
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

    if(hand.numbOfTrees < 1 || hand.depthOfTree < 0 || hand.numbOfBranches < 1 || hand.numbOfObjs < 0 || 
        strcmp(hand.daosObjClass, "S1") || strcmp(hand.collMetadata, "collective")) {
            H5_FAILED(); AT();
            printf("failed to do preorder tree traversal \n");
            goto error;
    }

    return 0;

error:
    return -1;
}

static void
initialize_time()
{
    int i;

    for (i = 0; i < ENTRY_NUM; i++) {
        op_time[i] = (double *)calloc(hand.numbOfTrees, sizeof(double));
        max_time[i] = 0;
        min_time[i] = 1000000.0;
    }

    for (i = 0; i < FILE_ENTRY_NUM; i++)
        file_op_time[i] = (double *)calloc(hand.numbOfObjs, sizeof(double));
}

static void
calculate_results()
{
    int i, j;
    double total_time[ENTRY_NUM];
    double overall_mean_time[ENTRY_NUM];
    double *mean_time_each_tree[ENTRY_NUM];
    int    total_nodes_per_tree = 0;
    double file_mean_time[FILE_ENTRY_NUM], file_total_time[FILE_ENTRY_NUM],
           file_max_time[FILE_ENTRY_NUM], file_min_time[FILE_ENTRY_NUM];

    /* Calculate results of objects */
    for (i = 0; i < ENTRY_NUM; i++)
        mean_time_each_tree[i] = (double *)calloc(hand.numbOfTrees, sizeof(double));

    if (hand.depthOfTree == 0) {
        total_nodes_per_tree = 1;

    } else {
        for (i = 0; i <= hand.depthOfTree; i++)
            total_nodes_per_tree += pow(hand.numbOfBranches, i);
    }

    memset(total_time, 0, sizeof(double) * ENTRY_NUM);

    for (i = 0; i < hand.numbOfTrees; i++) {
        for (j = 0; j < ENTRY_NUM; j++) {
            mean_time_each_tree[j][i] = op_time[j][i] / total_nodes_per_tree / hand.numbOfObjs;
            total_time[j] += mean_time_each_tree[j][i];
            overall_mean_time[j] = total_time[j] / hand.numbOfTrees;
 
            if(mean_time_each_tree[j][i] > max_time[j])
                max_time[j] = mean_time_each_tree[j][i];
            if(mean_time_each_tree[j][i] < min_time[j])
                min_time[j] = mean_time_each_tree[j][i];
        }
    }

    /* Calculate results of files */
    memset(file_total_time, 0, sizeof(double) * FILE_ENTRY_NUM);
    memset(file_max_time, 0, sizeof(double) * FILE_ENTRY_NUM);

    for (i = 0; i < FILE_ENTRY_NUM; i++)
        file_min_time[i] = 1000000.0;

    for (j = 0; j < FILE_ENTRY_NUM; j++) {
        for (i = 0; i < hand.numbOfObjs; i++) {
            file_total_time[j] += file_op_time[j][i];     

            if (file_op_time[j][i] > file_max_time[j])
                file_max_time[j] = file_op_time[j][i];
            if (file_op_time[j][i] < file_min_time[j])
                file_min_time[j] = file_op_time[j][i]; 
        }
       
        file_mean_time[j] = file_total_time[j] / hand.numbOfObjs;
    }

    if (MAINPROCESS) {
        printf("\nGroup creation time: 		min %lf, 	max %lf, 	mean %lf\n", min_time[GROUP_CREATE_NUM], max_time[GROUP_CREATE_NUM], 
	    overall_mean_time[GROUP_CREATE_NUM]);  
        printf("Group info time: 		min %lf, 	max %lf, 	mean %lf\n", min_time[GROUP_INFO_NUM], max_time[GROUP_INFO_NUM], 
	    overall_mean_time[GROUP_INFO_NUM]);  
        printf("Group removal time: 		min %lf, 	max %lf, 	mean %lf\n", min_time[GROUP_REMOVE_NUM], max_time[GROUP_REMOVE_NUM], 
	    overall_mean_time[GROUP_REMOVE_NUM]);  
        printf("Dataset creation time: 		min %lf, 	max %lf, 	mean %lf\n", min_time[DSET_CREATE_NUM], max_time[DSET_CREATE_NUM], 
	    overall_mean_time[DSET_CREATE_NUM]);  
        printf("Dataset read time: 		min %lf, 	max %lf, 	mean %lf\n", min_time[DSET_READ_NUM], max_time[DSET_READ_NUM], 
	    overall_mean_time[DSET_READ_NUM]);  
        printf("Dataset removal time: 		min %lf, 	max %lf, 	mean %lf\n", min_time[DSET_REMOVE_NUM], max_time[DSET_REMOVE_NUM], 
	    overall_mean_time[DSET_REMOVE_NUM]);  
        printf("Attribute creation time: 	min %lf, 	max %lf, 	mean %lf\n", min_time[ATTR_CREATE_NUM], max_time[ATTR_CREATE_NUM], 
	    overall_mean_time[ATTR_CREATE_NUM]);  
        printf("Attribute removal time: 	min %lf, 	max %lf, 	mean %lf\n", min_time[ATTR_REMOVE_NUM], max_time[ATTR_REMOVE_NUM], 
	    overall_mean_time[ATTR_REMOVE_NUM]);  
        printf("Datatype commit time: 		min %lf, 	max %lf, 	mean %lf\n", min_time[DTYPE_COMMIT_NUM], max_time[DTYPE_COMMIT_NUM], 
	    overall_mean_time[DTYPE_COMMIT_NUM]);  
        printf("Map creation time: 		min %lf, 	max %lf, 	mean %lf\n", min_time[MAP_CREATE_NUM], max_time[MAP_CREATE_NUM], 
	    overall_mean_time[MAP_CREATE_NUM]);  
        printf("Map removal time: 		min %lf, 	max %lf, 	mean %lf\n", min_time[MAP_REMOVE_NUM], max_time[MAP_REMOVE_NUM], 
	    overall_mean_time[MAP_REMOVE_NUM]);  
        printf("File creation time: 		min %lf, 	max %lf, 	mean %lf\n", file_min_time[FILE_CREATE_NUM], file_max_time[FILE_CREATE_NUM], 
	    file_mean_time[FILE_CREATE_NUM]);  
        printf("File removal time: 		min %lf, 	max %lf, 	mean %lf\n", file_min_time[FILE_REMOVE_NUM], file_max_time[FILE_REMOVE_NUM], 
	    file_mean_time[FILE_REMOVE_NUM]);  

        printf("\nGroup creation rate: 		max %lf, 	min %lf, 	mean %lf\n", 1 / min_time[GROUP_CREATE_NUM], 1 / max_time[GROUP_CREATE_NUM], 
	    1 / overall_mean_time[GROUP_CREATE_NUM]);  
        printf("Group info rate: 		max %lf, 	min %lf, 	mean %lf\n", 1 / min_time[GROUP_INFO_NUM], 1 / max_time[GROUP_INFO_NUM], 
	    1 / overall_mean_time[GROUP_INFO_NUM]);  
        printf("Group removal rate: 		max %lf, 	min %lf, 	mean %lf\n", 1 / min_time[GROUP_REMOVE_NUM], 1 / max_time[GROUP_REMOVE_NUM], 
	    1 / overall_mean_time[GROUP_REMOVE_NUM]);  
        printf("Dataset creation rate: 		max %lf, 	min %lf, 	mean %lf\n", 1 / min_time[DSET_CREATE_NUM], 1 / max_time[DSET_CREATE_NUM], 
	    1 / overall_mean_time[DSET_CREATE_NUM]);  
        printf("Dataset read rate: 		max %lf, 	min %lf, 	mean %lf\n", 1 / min_time[DSET_READ_NUM], 1 / max_time[DSET_READ_NUM], 
	    1 / overall_mean_time[DSET_READ_NUM]);  
        printf("Dataset removal rate: 		max %lf, 	min %lf, 	mean %lf\n", 1 / min_time[DSET_REMOVE_NUM], 1 / max_time[DSET_REMOVE_NUM], 
	    1 / overall_mean_time[DSET_REMOVE_NUM]); 
        printf("Attribute creation rate: 	max %lf, 	min %lf, 	mean %lf\n", 1 / min_time[ATTR_CREATE_NUM], 1 / max_time[ATTR_CREATE_NUM], 
	    1 / overall_mean_time[ATTR_CREATE_NUM]);  
        printf("Attribute removal rate: 	max %lf, 	min %lf, 	mean %lf\n", 1 / min_time[ATTR_REMOVE_NUM], 1 / max_time[ATTR_REMOVE_NUM], 
	    1 / overall_mean_time[ATTR_REMOVE_NUM]); 
        printf("Datatype commit rate: 		max %lf, 	min %lf, 	mean %lf\n", 1 / min_time[DTYPE_COMMIT_NUM], 1 / max_time[DTYPE_COMMIT_NUM], 
	    1 / overall_mean_time[DTYPE_COMMIT_NUM]);  
        printf("Map creation rate: 		max %lf, 	min %lf, 	mean %lf\n", 1 / min_time[MAP_CREATE_NUM], 1 / max_time[MAP_CREATE_NUM], 
	    1 / overall_mean_time[MAP_CREATE_NUM]);  
        printf("Map removal rate: 		max %lf, 	min %lf, 	mean %lf\n", 1 / min_time[MAP_REMOVE_NUM], 1 / max_time[MAP_REMOVE_NUM], 
	    1 / overall_mean_time[MAP_REMOVE_NUM]);  
        printf("File creation rate: 		max %lf, 	min %lf, 	mean %lf\n", 1 / file_min_time[FILE_CREATE_NUM], 1 / file_max_time[FILE_CREATE_NUM], 
	    1 / file_mean_time[FILE_CREATE_NUM]);  
        printf("File removal rate: 		max %lf, 	min %lf, 	mean %lf\n", 1 / file_min_time[FILE_REMOVE_NUM], 1 / file_max_time[FILE_REMOVE_NUM], 
	    1 / file_mean_time[FILE_REMOVE_NUM]);  
    }

    for (i = 0; i < ENTRY_NUM; i++)
        free(mean_time_each_tree[i]);
}

static void
free_time_struct()
{
    int i;

    for (i = 0; i < ENTRY_NUM; i++)
        free(op_time[i]);
}

static int 
create_objects_in_tree_node(hid_t tree_node_gid)
{
    hid_t gid = H5I_INVALID_HID, loc_id = H5I_INVALID_HID, dset_id = H5I_INVALID_HID, space_id = H5I_INVALID_HID;
    hid_t attr_id = H5I_INVALID_HID, dtype_id = H5I_INVALID_HID, map_id = H5I_INVALID_HID;
    char  gname[NAME_LENGTH], unique_group_per_rank_name[NAME_LENGTH], dset_name[NAME_LENGTH], attr_name[NAME_LENGTH];
    char  dtype_name[NAME_LENGTH], map_name[NAME_LENGTH];
    H5G_info_t group_info;
    hsize_t dim[DSET_RANK] = {DSET_DIM};
    int rbuf[DSET_DIM];
    int i;
    double start, end, time;

    /* Create a unique group for each rank (-u command-line option) */
    if (hand.uniqueGroupPerRank) {
        sprintf(unique_group_per_rank_name, "unique_group_per_rank_%d", mpi_rank);
        if ((loc_id = H5Gcreate2(tree_node_gid, unique_group_per_rank_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
            H5_FAILED(); AT();
            printf("    couldn't create group as tree root '%s'\n", unique_group_per_rank_name);
            goto error;
        }
    } else
        loc_id = tree_node_gid;

    /* Create group objects */
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(gname, "group_object_%d", i + 1);
        start = MPI_Wtime();

        if ((gid = H5Gcreate2(loc_id, gname, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
            H5_FAILED(); AT();
            printf("failed to create group object '%s'\n", gname);
            goto error;
        }

        end = MPI_Wtime();
        time = end - start;
        op_time[GROUP_CREATE_NUM][tree_order] += time;

#ifdef DEBUG
        if (MAINPROCESS)
            printf("\nGroup creation time: %lf", time);
#endif

        if (H5Gclose(gid) < 0) {
            H5_FAILED(); AT();
            printf("failed to close the group '%s'\n", gname);
            goto error;
        }
    }

    /* Group info */
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(gname, "group_object_%d", i + 1);

        start = MPI_Wtime();

        if (H5Gget_info_by_name(loc_id, gname, &group_info, H5P_DEFAULT) < 0) { 
            H5_FAILED(); AT();
            printf("failed to get info for the group '%s'\n", gname);
            goto error;
        }

        end = MPI_Wtime();
        time = end - start;
        op_time[GROUP_INFO_NUM][tree_order] += time;

#ifdef DEBUG
        if (MAINPROCESS)
            printf("\nGroup info time: %lf", time);
#endif
    }

    /* Group removal */
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(gname, "group_object_%d", i + 1);

        start = MPI_Wtime();
        if (H5Ldelete(loc_id, gname, H5P_DEFAULT) < 0) { 
            H5_FAILED(); AT();
            printf("failed to remove the group '%s'\n", gname);
            goto error;
        }

        end = MPI_Wtime();
        time = end - start;
        op_time[GROUP_REMOVE_NUM][tree_order] += time;

#ifdef DEBUG
        if (MAINPROCESS)
            printf("\nGroup removal time: %lf", time);
#endif
    }

    /* Create data space for datasets and attributes */
    if ((space_id = H5Screate_simple(DSET_RANK, dim, NULL)) < 0) {
        H5_FAILED(); AT();
        printf("failed to create data space\n");
        goto error;
    }

    /* Create dataset object */
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(dset_name, "dset_object_%d", i + 1);
        start = MPI_Wtime();

        if ((dset_id = H5Dcreate2(loc_id, dset_name, H5T_NATIVE_INT, space_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
            H5_FAILED(); AT();
            printf("failed to create dataset object '%s'\n", dset_name);
            goto error;
        }

        end = MPI_Wtime();
        time = end - start;
        op_time[DSET_CREATE_NUM][tree_order] += time;

#ifdef DEBUG
        if (MAINPROCESS)
            printf("\nDset creation time: %lf", time);
#endif

        /* Dataset read */
        start = MPI_Wtime();

        if (H5Dread(dset_id, H5T_NATIVE_INT, H5S_ALL, space_id, H5P_DEFAULT, &rbuf) < 0) {
            H5_FAILED(); AT();
            printf("failed to read the dataset '%s'\n", dset_name);
            goto error;
        }

        end = MPI_Wtime();
        time = end - start;
        op_time[DSET_READ_NUM][tree_order] += time;

        if (H5Dclose(dset_id) < 0) {
            H5_FAILED(); AT();
            printf("failed to close the dataset '%s'\n", dset_name);
            goto error;
        }
    }

    /* Dataset removal */
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(dset_name, "dset_object_%d", i + 1);

        start = MPI_Wtime();
        if (H5Ldelete(loc_id, dset_name, H5P_DEFAULT) < 0) { 
            H5_FAILED(); AT();
            printf("failed to remove the dataset '%s'\n", dset_name);
            goto error;
        }

        end = MPI_Wtime();
        time = end - start;
        op_time[DSET_REMOVE_NUM][tree_order] += time;

#ifdef DEBUG
        if (MAINPROCESS)
            printf("\nDataset removal time: %lf", time);
#endif
    }

    /* Attribute creation */
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(attr_name, "attribute_object_%d", i + 1);
        start = MPI_Wtime();

        if ((attr_id = H5Acreate2(loc_id, attr_name, H5T_NATIVE_INT, space_id, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
            H5_FAILED(); AT();
            printf("failed to create attribute object '%s'\n", attr_name);
            goto error;
        }

        end = MPI_Wtime();
        time = end - start;
        op_time[ATTR_CREATE_NUM][tree_order] += time;

#ifdef DEBUG
        if (MAINPROCESS)
            printf("\nAttr creation time: %lf", time);
#endif

        if(H5Aclose(attr_id) < 0) {
            H5_FAILED(); AT();
            printf("failed to close the attribure\n");
            goto error;
        } 
    }

    /* Close the data sapce for datasets and attributes */
    if (H5Sclose(space_id) < 0) {
        H5_FAILED(); AT();
        printf("failed to close the data space\n");
        goto error;
    }

    /* Attribute removal */
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(attr_name, "attribute_object_%d", i + 1);
        start = MPI_Wtime();

        if (H5Adelete(loc_id, attr_name) < 0) { 
            H5_FAILED(); AT();
            printf("failed to remove the attribute '%s'\n", attr_name);
            goto error;
        }

        end = MPI_Wtime();
        time = end - start;
        op_time[ATTR_REMOVE_NUM][tree_order] += time;

#ifdef DEBUG
        if (MAINPROCESS)
            printf("\nAttr removal time: %lf", time);
#endif
    }

    /* Datatype commit */
    for (i = 0; i < hand.numbOfObjs; i++) {
        if ((dtype_id = H5Tcopy(H5T_NATIVE_INT)) < 0) {
            H5_FAILED(); AT();
            printf("failed to copy a datatype\n");
            goto error;
        }

        sprintf(dtype_name, "dtype_object_%d", i + 1);
        start = MPI_Wtime();

        if (H5Tcommit2(loc_id, dtype_name, dtype_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) < 0) {
            H5_FAILED(); AT();
            printf("failed to commit datatype object '%s'\n", dtype_name);
            goto error;
        }

        end = MPI_Wtime();
        time = end - start;
        op_time[DTYPE_COMMIT_NUM][tree_order] += time;

#ifdef DEBUG
        if (MAINPROCESS)
            printf("\nDtype commit time: %lf", time);
#endif

        if (H5Tclose(dtype_id) < 0) {
            H5_FAILED(); AT();
            printf("failed to close the datatype\n");
            goto error;
        } 
    }

    if(!hand.runMPIIO) {
        /* Map creation (only works for H5VOL) */
        for (i = 0; i < hand.numbOfObjs; i++) {
            sprintf(map_name, "map_object_%d", i + 1);
            start = MPI_Wtime();

            if ((map_id = H5Mcreate(loc_id, map_name, H5T_NATIVE_INT, H5T_NATIVE_INT, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
                H5_FAILED(); AT();
                printf("failed to create map object '%s'\n", map_name);
                goto error;
            }

            end = MPI_Wtime();
            time = end - start;
            op_time[MAP_CREATE_NUM][tree_order] += time;

#ifdef DEBUG
            if (MAINPROCESS)
                printf("\nMap creation time: %lf", time);
#endif

            if(H5Mclose(map_id) < 0) {
                H5_FAILED(); AT();
                printf("failed to close the map\n");
                goto error;
            } 
        }

        /* Map removal */
        for (i = 0; i < hand.numbOfObjs; i++) {
            sprintf(map_name, "map_object_%d", i + 1);

            start = MPI_Wtime();
            if (H5Ldelete(loc_id, map_name, H5P_DEFAULT) < 0) { 
                H5_FAILED(); AT();
                printf("failed to remove the map '%s'\n", map_name);
                goto error;
            }

            end = MPI_Wtime();
            time = end - start;
            op_time[MAP_REMOVE_NUM][tree_order] += time;

#ifdef DEBUG
            if (MAINPROCESS)
                printf("\nMap removal time: %lf", time);
#endif
        }
    }

    /* Close the unique group per rank */
    if (hand.uniqueGroupPerRank) {
        if (H5Gclose(loc_id) < 0) {
            H5_FAILED(); AT();
            printf("failed to close the unique group per rank '%s'\n", unique_group_per_rank_name);
            goto error;
        }
    }

    return 0;

error:
    return -1;
}

static int
operate_on_files(hid_t fapl_id) 
{
    hid_t file_id;
    char filename[NAME_LENGTH];
    double start, end, time;
    int i;

    /* File creation */
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(filename, "%s_%d", hand.fileName, i + 1);
        start = MPI_Wtime();

        if((file_id = H5Fcreate(filename, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0) {
            H5_FAILED(); AT();
            printf("failed to create the file '%s'\n", filename);
            goto error;
        }

        end = MPI_Wtime();
        time = end - start;
        file_op_time[FILE_CREATE_NUM][i] = time;

#ifdef DEBUG
        if (MAINPROCESS)
            printf("\nFile creation time: %lf", time);
#endif

        if(H5Fclose(file_id) < 0) {
            H5_FAILED(); AT();
            printf("failed to close the file\n");
            goto error;
        } 
    }

    /* File delete */
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(filename, "%s_%d", hand.fileName, i + 1);
        start = MPI_Wtime();

        /* H5Fdelete only works for H5VOL */
        if(!hand.runMPIIO) {
            if ((file_id = H5Fdelete(filename, fapl_id)) < 0) {
                H5_FAILED(); AT();
                printf("failed to delete the file '%s'\n", filename);
                goto error;
            }
        } else {
            if (MPI_File_delete(filename, MPI_INFO_NULL)) {
                H5_FAILED(); AT();
                printf("failed to delete the file '%s'\n", filename);
                goto error;
            }
        }

        end = MPI_Wtime();
        time = end - start;
        file_op_time[FILE_REMOVE_NUM][i] = time;

#ifdef DEBUG
        if (MAINPROCESS)
            printf("\nFile removal time: %lf", time);
#endif

    }

    return 0;

error:
    return -1;

}

/* Recursive function to create tree of the depth (depthOfTree) */
static int
create_tree_preorder(hid_t parent_gid, unsigned depth)
{
    hid_t child_gid = H5I_INVALID_HID;
    char  gname[NAME_LENGTH];
    int   i;

    for(i = 0; i < hand.numbOfBranches; i++) {
        sprintf(gname, "child_group_depth_%d_branch_%d", depth, i + 1);

        if ((child_gid = H5Gcreate2(parent_gid, gname, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
            H5_FAILED(); AT();
            printf("    couldn't create group '%s'\n", gname);
            goto error;
        }

        if (create_objects_in_tree_node(child_gid) < 0) {
            H5_FAILED(); AT();
            printf("    couldn't create objects in the tree root node\n");
            goto error;
        }

        if (depth < (unsigned)hand.depthOfTree) {
            if (create_tree_preorder(child_gid, depth+1) < 0) {
                H5_FAILED(); AT();
                printf("failed to do preorder tree traversal \n");
                goto error;
            }
        }

        if (H5Gclose(child_gid) < 0) {
            H5_FAILED(); AT();
            printf("failed to close the group '%s'\n", gname);
            goto error;
        }
    }

    return 0;

error:
    H5E_BEGIN_TRY {
        H5Gclose(child_gid);
    } H5E_END_TRY;

    return -1;
}

static hbool_t create_trees(hid_t file) {
    hid_t       tree_root_id;
    char        tree_root_name[NAME_LENGTH];
    int         i;

    for(i = 0; i < hand.numbOfTrees; i++) {
        snprintf(tree_root_name, NAME_LENGTH, "tree_root_%d", i+1);
        tree_order = i;

        /* Create the group as the tree root */
        if ((tree_root_id = H5Gcreate2(file, tree_root_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
            H5_FAILED(); AT();
            printf("    couldn't create group as tree root '%s'\n", tree_root_name);
            goto error;
        }

        if (create_objects_in_tree_node(tree_root_id) < 0) {
            H5_FAILED(); AT();
            printf("    couldn't create objects in the tree root node\n");
            goto error;
        }

        if (0 < hand.depthOfTree) {
            if (create_tree_preorder(tree_root_id, 1) < 0) {
                H5_FAILED(); AT();
                printf("    couldn't create tree branches\n");
                goto error;
            }
        }

        if (H5Gclose(tree_root_id) < 0) {
            H5_FAILED(); AT();
            printf("failed to close the group '%s' as the tree root\n", tree_root_name);
            goto error;
        }
    }

    return 0;

error:

    return -1;
}

/*
 * main function
 */
int
main( int argc, char** argv )
{
    hid_t   fapl_id = -1, file_id = -1;
    int     nerrors = 0;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD,&mpi_size);

    if (parse_command_line(argc, argv) < 0) {
        nerrors++;
        goto error;
    }

    initialize_time();

    if ((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0) {
        nerrors++;
        goto error;
    }

    if (H5Pset_fapl_mpio(fapl_id, MPI_COMM_WORLD, MPI_INFO_NULL) < 0) {
        nerrors++;
        goto error;
    }

    if(H5Pset_all_coll_metadata_ops(fapl_id, TRUE) < 0) {
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

    if((file_id = H5Fcreate(hand.fileName, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0) {
        nerrors++;
        goto error;
    }

    /* Create trees */
    if(create_trees(file_id) < 0) {
        nerrors++;
        goto error;
    }

    if(H5Fclose(file_id) < 0) {
        nerrors++;
        goto error;
    }

    /* Create and delete files */
    if(operate_on_files(fapl_id) < 0) {
        nerrors++;
        goto error;
    }

    if(H5Pclose(fapl_id) < 0) {
        nerrors++;
        goto error;
    }

    calculate_results();
    free_time_struct();

    if (nerrors) goto error;

    if (MAINPROCESS) {
        puts("========================\n");
        puts("\n\nAll tests passed");
    }

    MPI_Finalize();

    return 0;

error:
    if (MAINPROCESS) printf("*** %d TEST%s FAILED ***\n", nerrors, (!nerrors || nerrors > 1) ? "S" : "");

    MPI_Finalize();

    return 1;
} /* end main() */
