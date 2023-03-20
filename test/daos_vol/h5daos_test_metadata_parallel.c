/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

/**
 * Purpose: Performance benchmark for metadata
 */

#include "h5daos_test.h"

#include "daos_vol.h"

/*
 * Definitions
 */
//#define DEBUG
#define TRUE  1
#define FALSE 0

#define FILENAME    "h5daos_test_mdata.h5"
#define NAME_LENGTH 256
#define DSET_RANK   2
#define ATTR_RANK   1

/* Struct for command-line options */
typedef struct {
    int     numbOfTrees;
    int     depthOfTree;
    int     numbOfBranches;
    int     numbOfObjs;
    int     numbOfFiles;
    int     dset_dim1;
    int     dset_dim2;
    int     chunk_dim1;
    int     chunk_dim2;
    char   *dset_dtype;
    char   *dset_layout;
    char   *map_dtype;
    int     attr_dim;
    int     numbOfMapEntries;
    int     numbOfNestedGroups;
    int     waitInterval;
    hbool_t uniqueGroupPerRank;
    hbool_t runMPIIO;
    hbool_t testAllObjects;
    hbool_t testFileOnly;
    hbool_t testGroupOnly;
    hbool_t testDsetOnly;
    hbool_t testAttrOnly;
    hbool_t testDtypeOnly;
    hbool_t testObjectOnly;
    hbool_t testMapOnly;
    hbool_t isAsync;
    hbool_t useWait;
    hbool_t trackOrder;
    hbool_t noGroupMember;
    hbool_t noWriteReadData;
    hbool_t noMapEntry;
    char   *fileName;
} handler_t;

/* List of object operations */
typedef enum {
    GROUP_CREATE_NUM          = 0,
    GROUP_INFO_NUM            = 1,
    GROUP_OPEN_NUM            = 2,
    GROUP_CLOSE_NUM           = 3,
    GROUP_REMOVE_NUM          = 4,
    DSET_CREATE_NUM           = 5,
    DSET_OPEN_NUM             = 6,
    DSET_WRITE_NUM            = 7,
    DSET_READ_NUM             = 8,
    DSET_INFO_NUM             = 9,
    DSET_CLOSE_NUM            = 10,
    DSET_CREATEWRITECLOSE_NUM = 11,
    DSET_OPENREADCLOSE_NUM    = 12,
    DSET_REMOVE_NUM           = 13,
    ATTR_CREATE_NUM           = 14,
    ATTR_OPEN_NUM             = 15,
    ATTR_WRITE_NUM            = 16,
    ATTR_READ_NUM             = 17,
    ATTR_CLOSE_NUM            = 18,
    ATTR_REMOVE_NUM           = 19,
    DTYPE_COMMIT_NUM          = 20,
    DTYPE_OPEN_NUM            = 21,
    DTYPE_CLOSE_NUM           = 22,
    MAP_CREATE_NUM            = 23,
    MAP_PUT_NUM               = 24,
    MAP_GET_NUM               = 25,
    MAP_OPEN_NUM              = 26,
    MAP_CLOSE_NUM             = 27,
    MAP_REMOVE_NUM            = 28,
    LINK_ITERATE_NUM          = 29,
    LINK_EXIST_NUM            = 30,
    OBJ_OPEN_GRP_NUM          = 31,
    OBJ_COPY_GRP_NUM          = 32,
    ENTRY_NUM
} test_num_t;

/* List of operations for printing results. Must match the order of test_num_t above */
const char *metadata_op[] = {"Group create rate",
                             "Group info rate",
                             "Group open rate",
                             "Group close rate",
                             "Group remove rate",
                             "Dset create rate",
                             "Dset open rate",
                             "Dset write rate",
                             "Dset read rate",
                             "Dset info rate",
                             "Dset close rate",
                             "Dset create+write+close rate",
                             "Dset open+read+close rate",
                             "Dset remove rate",
                             "Attr create rate",
                             "Attr open rate",
                             "Attr write rate",
                             "Attr read rate",
                             "Attr close rate",
                             "Attr remove rate",
                             "Dtype commit rate",
                             "Dtype open rate",
                             "Dtype close rate",
                             "Map create rate",
                             "Map put rate",
                             "Map get rate",
                             "Map open rate",
                             "Map close rate",
                             "Map remove rate",
                             "Link iterate rate",
                             "Link exist rate",
                             "Object open rate (groups)",
                             "Object copy rate (groups)"};

/* List of file operations */
typedef enum {
    FILE_CREATE_NUM = 0,
    FILE_OPEN_NUM   = 1,
    FILE_CLOSE_NUM  = 2,
    FILE_REMOVE_NUM = 3,
    FILE_ENTRY_NUM
} file_num_t;

const char *file_op[] = {"File create rate", "File open rate", "File close rate", "File remove rate"};

typedef struct {
    double max_rate[ENTRY_NUM];
    double min_rate[ENTRY_NUM];
    double mean_rate[ENTRY_NUM];

    double total_max_rate[ENTRY_NUM];
    double total_min_rate[ENTRY_NUM];
    double total_mean_rate[ENTRY_NUM];
    double avg_max_rate[ENTRY_NUM];
    double avg_min_rate[ENTRY_NUM];
    double avg_mean_rate[ENTRY_NUM];

    double file_mean_time[FILE_ENTRY_NUM];
    double file_max_time[FILE_ENTRY_NUM];
    double file_min_time[FILE_ENTRY_NUM];

    double file_mean_rate[FILE_ENTRY_NUM];
    double file_max_rate[FILE_ENTRY_NUM];
    double file_min_rate[FILE_ENTRY_NUM];
} results_t;

/* Global variables */
int              mpi_rank;
int              mpi_size;
static handler_t hand;
results_t        results;
double          *op_time[ENTRY_NUM];
double          *file_op_time[FILE_ENTRY_NUM];
unsigned         tree_order;
hid_t            file_id;
hid_t            file_dspace, file_dspace_select, mem_space;
hid_t            attr_space;
hid_t            dcpl_id, gcpl_id, gapl_id;
int             *map_keys, *map_vals, *map_vals_out;
int             *wdata, *rdata;
char            *wdata_char, *rdata_char;
long long       *wdata_llong, *rdata_llong;
float           *wdata_float, *rdata_float;
double          *wdata_double, *rdata_double;
int             *attr_write, *attr_read;
/* Keys and values for int-int map */
int *map_keys, *map_vals, *map_vals_out;
/* Keys and values for vls-vl map */
char **vls_vl_keys;
hvl_t *vls_vl_vals;
hvl_t *vls_vl_out;
hid_t  map_vl_key_dtype_id, map_vl_value_dtype_id;

/* Show command usage */
static void
usage(void)
{
    printf("    [-h] [-a] [-A] [-b] [-c] [-d] [-D] [-e] [-f] [-F] [-g] [-G] [-i] [-I] [-j] [-k] [-l] [-m] "
           "[-M] [-n] [-o] [-p] [-r] [-s] [-t] [-T] [-u] [-w] [-z]\n\n");

    printf("    [-h]: this help page\n");
    printf("    [-a]: indicate to run collective metadata I/O (H5MPIIO as the backend)\n");
    printf("    [-A]: Run the test for attribute object only\n");
    printf("    [-b]: the number of branches per tree node\n");
    printf("    [-c]: the 2D dimensions of the dataset chunk size (16x16 is the default), e.g. 8x8\n");
    printf("    [-d]: the 2D dimensions of the datasets (16x16 is the default), e.g. 16x32\n");
    printf("    [-D]: Run the test for dataset object only\n");
    printf("    [-e]: the number of entries per map\n");
    printf("    [-f]: the single dimension of the attributes\n");
    printf("    [-F]: Run the test for file object only\n");
    printf("    [-g]: the datatype of the dataset\n");
    printf("    [-G]: Run the test for group object only\n");
    printf("    [-i]: the number of trees (iterations) for object operations\n");
    printf("    [-I]: the number of objects (groups, datasets, attributes, and maps) per tree node\n");
    printf("    [-j]: the number of subgroups in the group objects\n");
    printf("    [-k]: track the creation order for the parent group where all objects are created under\n");
    printf("    [-l]: the layout of datasets (contiguous is the default), e.g. chunked or compact\n");
    printf("    [-m]: the datatype for map object\n");
    printf("    [-M]: Run the test for map object only\n");
    printf("    [-n]: the number of iterations for file operations\n");
    printf("    [-o]: File name (add the prefix 'daos:' for H5MPIIO)\n");
    printf("    [-O]: Run the test for HDF5's H5O API only\n");
    printf("    [-p]: the wait interval for async I/O during the loop \n");
    printf("    [-r]: No write or read the datasets\n");
    printf("    [-s]: No entry for maps\n");
    printf("    [-t]: No member in groups\n");
    printf("    [-T]: Run the test for named datatype object only\n");
    printf("    [-u]: unique group per rank where objects will be located under.  Option -a shouldn't be set "
           "at the same time.\n");
    printf("	      It'll use independent metadata I/O.  If not set, collective I/O will be used\n");
    printf("    [-U]: use Async I/O, otherwise use sync I/O if not specified. It should be used with -u "
           "only, not with -a.\n");
    printf("	[-w]: put an H5ESwait call after the operations to make async function behave similar to "
           "sync function.\n");
    printf("    [-z]: the number of levels (depth) for the tree (the tree root is at level 0) \n");
    printf("\n");
}

static int
parse_command_line(int argc, char *argv[])
{
    int opt;

    /* Initialize the command line options */
    hand.numbOfTrees        = 10;
    hand.depthOfTree        = 0;
    hand.numbOfBranches     = 3;
    hand.numbOfObjs         = 200;
    hand.dset_dim1          = 16;
    hand.dset_dim2          = 16;
    hand.chunk_dim1         = 16;
    hand.chunk_dim2         = 16;
    hand.numbOfMapEntries   = 16;
    hand.attr_dim           = 16;
    hand.numbOfNestedGroups = 16;
    hand.numbOfFiles        = 16;
    hand.waitInterval       = 20;
    hand.uniqueGroupPerRank = FALSE;
    hand.runMPIIO           = FALSE;
    hand.testAllObjects     = TRUE;
    hand.testFileOnly       = FALSE;
    hand.testGroupOnly      = FALSE;
    hand.testDsetOnly       = FALSE;
    hand.testAttrOnly       = FALSE;
    hand.testDtypeOnly      = FALSE;
    hand.testObjectOnly     = FALSE;
    hand.testMapOnly        = FALSE;
    hand.noGroupMember      = FALSE;
    hand.noWriteReadData    = FALSE;
    hand.noMapEntry         = FALSE;
    hand.isAsync            = FALSE;
    hand.useWait            = FALSE;
    hand.trackOrder         = FALSE;
    hand.dset_dtype         = strdup("int");
    hand.map_dtype          = strdup("int");
    hand.dset_layout        = strdup("contiguous");
    hand.fileName           = strdup(FILENAME);

    while ((opt = getopt(argc, argv, "aAb:c:d:De:f:Fg:Ghi:I:j:l:m:Mn:o:Op:rstTuUwz:")) != -1) {
        switch (opt) {
            case 'a':
                /* Flag to indicate to use collective metadata I/O (H5MPIIO as the backend) */
                if (MAINPROCESS)
                    fprintf(stdout, "run H5MPIIO:	 				TRUE\n");
                hand.runMPIIO = TRUE;
                break;
            case 'A':
                /* Flag to indicate running test for attribute object alone */
                if (MAINPROCESS)
                    fprintf(stdout, "run test for attribute only: 				TRUE\n");
                hand.testAttrOnly   = TRUE;
                hand.testAllObjects = FALSE;
                break;
            case 'b':
                /* The number of branches for each group */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "branches of hierarchical tree: 				%s\n", optarg);
                    hand.numbOfBranches = atoi(optarg);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'c':
                /* The chunk dimension of the dataset */
                if (optarg) {
                    char *chunks_str, *dim1_str, *dim2_str;
                    if (MAINPROCESS)
                        fprintf(stdout, "chunk dimensions of datasets: 				%s\n", optarg);
                    chunks_str      = strdup(optarg);
                    dim1_str        = strtok(chunks_str, "x");
                    dim2_str        = strtok(NULL, "x");
                    hand.chunk_dim1 = atoi(dim1_str);
                    hand.chunk_dim2 = atoi(dim2_str);
                    free(chunks_str);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'd':
                /* The dimensions of the dataset */
                if (optarg) {
                    char *dims_str, *dim1_str, *dim2_str;
                    if (MAINPROCESS)
                        fprintf(stdout, "dimensions of datasets: 				%s\n", optarg);
                    dims_str       = strdup(optarg);
                    dim1_str       = strtok(dims_str, "x");
                    dim2_str       = strtok(NULL, "x");
                    hand.dset_dim1 = atoi(dim1_str);
                    hand.dset_dim2 = atoi(dim2_str);
                    free(dims_str);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'D':
                /* Flag to indicate running test for dataset object alone */
                if (MAINPROCESS)
                    fprintf(stdout, "run test for dataset only:	 					TRUE\n");
                hand.testDsetOnly   = TRUE;
                hand.testAllObjects = FALSE;
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
            case 'f':
                /* The single dimension of attributes */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "single dimension of attributes: 				%s\n", optarg);
                    hand.attr_dim = atoi(optarg);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'F':
                /* Flag to indicate running test for file alone */
                if (MAINPROCESS)
                    fprintf(stdout, "run test for file only:	 					TRUE\n");
                hand.testFileOnly   = TRUE;
                hand.testAllObjects = FALSE;
                break;
            case 'g':
                /* Dataset's datatype */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "Dataset datatype: 					%s\n", optarg);
                    if (hand.dset_dtype)
                        free(hand.dset_dtype);
                    hand.dset_dtype = strdup(optarg);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'G':
                /* Flag to indicate running test for group object (including link) alone */
                if (MAINPROCESS)
                    fprintf(stdout, "run test for group only:	 					TRUE\n");
                hand.testGroupOnly  = TRUE;
                hand.testAllObjects = FALSE;
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
                /* The number of iterations (trees) for the test */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "number of iterations (trees): 				%s\n", optarg);
                    hand.numbOfTrees = atoi(optarg);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'I':
                /* The number of objects (groups, datasets, attributes, and maps) per node */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "number of objects per tree node: 			%s\n", optarg);
                    hand.numbOfObjs = atoi(optarg);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'j':
                /* The number of subgroups in each group objects */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "number of subgroups:	 				%s\n", optarg);
                    hand.numbOfNestedGroups = atoi(optarg);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'k':
                /* Flag to indicate tracking the creation order for the parent group where all objects are
                 * created under */
                if (MAINPROCESS)
                    fprintf(stdout, "track the creation order for the parent group:		TRUE\n");
                hand.trackOrder = TRUE;
                break;
            case 'l':
                /* Dataset layout */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "Dataset layout: 					%s\n", optarg);
                    if (hand.dset_layout)
                        free(hand.dset_layout);
                    hand.dset_layout = strdup(optarg);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'm':
                /* The datatype of map entry */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "Map datatype: 						%s\n", optarg);
                    if (hand.map_dtype)
                        free(hand.map_dtype);
                    hand.map_dtype = strdup(optarg);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'M':
                /* Flag to indicate running test for map object alone */
                if (MAINPROCESS)
                    fprintf(stdout, "run test for map only: 				TRUE\n");
                hand.testMapOnly    = TRUE;
                hand.testAllObjects = FALSE;
                break;
            case 'n':
                /* The number of files for file operations (create, open, close, and delete) */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "number of file for file operations: 			%s\n", optarg);
                    hand.numbOfFiles = atoi(optarg);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'o':
                /* File name (add the prefix 'daos:' for H5MPIIO) */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "HDF5 file name: 					%s\n", optarg);
                    if (hand.fileName)
                        free(hand.fileName);
                    hand.fileName = strdup(optarg);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'O':
                /* Flag to indicate running test for H5O alone */
                if (MAINPROCESS)
                    fprintf(stdout, "run test for H5O only:                       TRUE\n");
                hand.testObjectOnly = TRUE;
                hand.testAllObjects = FALSE;
                break;
            case 'p':
                /* The wait interval for async I/O */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "wait interval for async I/O: 				%s\n", optarg);
                    hand.waitInterval = atoi(optarg);
                }
                else
                    printf("optarg is null\n");
                break;
            case 'r':
                /* No write or read the datasets */
                if (MAINPROCESS)
                    fprintf(stdout, "no write or read datasets: 				TRUE\n");
                hand.noWriteReadData = TRUE;
                break;
            case 's':
                /* No entry for maps */
                if (MAINPROCESS)
                    fprintf(stdout, "no entry for maps:		 				TRUE\n");
                hand.noMapEntry = TRUE;
                break;
            case 't':
                /* No member for groups */
                if (MAINPROCESS)
                    fprintf(stdout, "no member in groups:	 				TRUE\n");
                hand.noGroupMember = TRUE;
                break;
            case 'T':
                /* Flag to indicate running test for named datatype object alone */
                if (MAINPROCESS)
                    fprintf(stdout, "run test for named datatype only: 				TRUE\n");
                hand.testDtypeOnly  = TRUE;
                hand.testAllObjects = FALSE;
                break;
            case 'u':
                /* Unique group per rank where objects will be located under. Independent I/O will be used  */
                if (MAINPROCESS)
                    fprintf(stdout, "unique group per rank: 					TRUE\n");
                hand.uniqueGroupPerRank = TRUE;
                break;
            case 'U':
                /* Flag to indicate using Async I/O */
                if (MAINPROCESS)
                    fprintf(stdout, "use Async I/O: 						TRUE\n");
                hand.isAsync = TRUE;
                break;
            case 'w':
                /* Put an H5ESwait after an async function call to make it behave similar to sync function */
                if (MAINPROCESS)
                    fprintf(stdout, "use H5ESwait after object creation: 			TRUE\n");
                hand.useWait = TRUE;
                break;
            case 'z':
                /* The number of levels (depth) for the tree */
                if (optarg) {
                    if (MAINPROCESS)
                        fprintf(stdout, "depth of hierarchical groups (tree): 			%s\n", optarg);
                    hand.depthOfTree = atoi(optarg);
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

    if (hand.numbOfTrees < 1 || hand.depthOfTree < 0 || hand.numbOfBranches < 1 || hand.numbOfObjs < 0 ||
        hand.numbOfFiles < 0 || hand.waitInterval < 1) {
        H5_FAILED();
        AT();
        printf("invalid command-line option value \n");
        goto error;
    }

    if (hand.runMPIIO && hand.uniqueGroupPerRank) {
        H5_FAILED();
        AT();
        printf("invalid command-line option value: unique group can't be enable for running H5MPIIO\n");
        goto error;
    }

    if (hand.isAsync && !hand.uniqueGroupPerRank) {
        H5_FAILED();
        AT();
        printf(
            "invalid command-line option value: Async functions must be used with unique group per rank\n");
        goto error;
    }

    if (hand.useWait && !hand.isAsync) {
        H5_FAILED();
        AT();
        printf("invalid command-line option value: H5ESwait must be used with async functions\n");
        goto error;
    }

    /* Dataset layout must be either chunked, contiguous, or compact */
    if (strcmp(hand.dset_layout, "chunked") && strcmp(hand.dset_layout, "contiguous") &&
        strcmp(hand.dset_layout, "compact")) {
        H5_FAILED();
        AT();
        printf("invalid dataset layout: %s\n", hand.dset_layout);
        goto error;
    }

    /* Dataset's datatype must be either char, int, long long, float, or double */
    if (strcmp(hand.dset_dtype, "char") && strcmp(hand.dset_dtype, "int") &&
        strcmp(hand.dset_dtype, "llong") && strcmp(hand.dset_dtype, "float") &&
        strcmp(hand.dset_dtype, "double")) {
        H5_FAILED();
        AT();
        printf("invalid dataset's datatype: %s\n", hand.dset_dtype);
        goto error;
    }

    /* Map's datatype must be either int or vl */
    if (strcmp(hand.map_dtype, "int") && strcmp(hand.map_dtype, "vl")) {
        H5_FAILED();
        AT();
        printf("invalid map datatype: %s\n", hand.map_dtype);
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

    for (i = 0; i < ENTRY_NUM; i++)
        op_time[i] = (double *)calloc((size_t)hand.numbOfTrees, sizeof(double));

    for (i = 0; i < FILE_ENTRY_NUM; i++)
        file_op_time[i] = (double *)calloc((size_t)hand.numbOfObjs, sizeof(double));
}

static void
initialize_data()
{
    int i, j;

    /* Allocate the memory for the dataset write and read */
    if (!hand.uniqueGroupPerRank) {
        if (!strcmp(hand.dset_dtype, "int")) {
            wdata = (int *)malloc(((size_t)hand.dset_dim1 / (size_t)mpi_size) * (size_t)hand.dset_dim2 *
                                  sizeof(int));
            rdata = (int *)malloc(((size_t)hand.dset_dim1 / (size_t)mpi_size) * (size_t)hand.dset_dim2 *
                                  sizeof(int));
        }
        else if (!strcmp(hand.dset_dtype, "char")) {
            wdata_char = (char *)malloc(((size_t)hand.dset_dim1 / (size_t)mpi_size) * (size_t)hand.dset_dim2 *
                                        sizeof(char));
            rdata_char = (char *)malloc(((size_t)hand.dset_dim1 / (size_t)mpi_size) * (size_t)hand.dset_dim2 *
                                        sizeof(char));
        }
        else if (!strcmp(hand.dset_dtype, "llong")) {
            wdata_llong = (long long *)malloc(((size_t)hand.dset_dim1 / (size_t)mpi_size) *
                                              (size_t)hand.dset_dim2 * sizeof(long long));
            rdata_llong = (long long *)malloc(((size_t)hand.dset_dim1 / (size_t)mpi_size) *
                                              (size_t)hand.dset_dim2 * sizeof(long long));
        }
        else if (!strcmp(hand.dset_dtype, "float")) {
            wdata_float = (float *)malloc(((size_t)hand.dset_dim1 / (size_t)mpi_size) *
                                          (size_t)hand.dset_dim2 * sizeof(float));
            rdata_float = (float *)malloc(((size_t)hand.dset_dim1 / (size_t)mpi_size) *
                                          (size_t)hand.dset_dim2 * sizeof(float));
        }
        else if (!strcmp(hand.dset_dtype, "double")) {
            wdata_double = (double *)malloc(((size_t)hand.dset_dim1 / (size_t)mpi_size) *
                                            (size_t)hand.dset_dim2 * sizeof(double));
            rdata_double = (double *)malloc(((size_t)hand.dset_dim1 / (size_t)mpi_size) *
                                            (size_t)hand.dset_dim2 * sizeof(double));
        }
    }
    else {
        if (!strcmp(hand.dset_dtype, "int")) {
            wdata = (int *)malloc((size_t)hand.dset_dim1 * (size_t)hand.dset_dim2 * sizeof(int));
            rdata = (int *)malloc((size_t)hand.dset_dim1 * (size_t)hand.dset_dim2 * sizeof(int));
        }
        else if (!strcmp(hand.dset_dtype, "char")) {
            wdata_char = (char *)malloc((size_t)hand.dset_dim1 * (size_t)hand.dset_dim2 * sizeof(char));
            rdata_char = (char *)malloc((size_t)hand.dset_dim1 * (size_t)hand.dset_dim2 * sizeof(char));
        }
        else if (!strcmp(hand.dset_dtype, "llong")) {
            wdata_llong =
                (long long *)malloc((size_t)hand.dset_dim1 * (size_t)hand.dset_dim2 * sizeof(long long));
            rdata_llong =
                (long long *)malloc((size_t)hand.dset_dim1 * (size_t)hand.dset_dim2 * sizeof(long long));
        }
        else if (!strcmp(hand.dset_dtype, "float")) {
            wdata_float = (float *)malloc((size_t)hand.dset_dim1 * (size_t)hand.dset_dim2 * sizeof(float));
            rdata_float = (float *)malloc((size_t)hand.dset_dim1 * (size_t)hand.dset_dim2 * sizeof(float));
        }
        else if (!strcmp(hand.dset_dtype, "double")) {
            wdata_double = (double *)malloc((size_t)hand.dset_dim1 * (size_t)hand.dset_dim2 * sizeof(double));
            rdata_double = (double *)malloc((size_t)hand.dset_dim1 * (size_t)hand.dset_dim2 * sizeof(double));
        }
    }

    /* Initialize the data for the dataset */
    for (i = 0; i < hand.dset_dim1 / mpi_size; i++) {
        for (j = 0; j < hand.dset_dim2; j++) {
            if (!strcmp(hand.dset_dtype, "int"))
                *(wdata + i * hand.dset_dim2 + j) = i + j;
            else if (!strcmp(hand.dset_dtype, "char"))
                *(wdata_char + i * hand.dset_dim2 + j) = (char)((i + j) % 128);
            else if (!strcmp(hand.dset_dtype, "llong"))
                *(wdata_llong + i * hand.dset_dim2 + j) = (long long)(i + j);
            else if (!strcmp(hand.dset_dtype, "float"))
                *(wdata_float + i * hand.dset_dim2 + j) = (float)(i + j);
            else if (!strcmp(hand.dset_dtype, "double"))
                *(wdata_double + i * hand.dset_dim2 + j) = (double)(i + j);
        }
    }

    /* Allocate the memory for the map entries */
    if (!strcmp(hand.map_dtype, "int")) {
        map_keys     = (int *)malloc((size_t)hand.numbOfMapEntries * sizeof(int));
        map_vals     = (int *)malloc((size_t)hand.numbOfMapEntries * sizeof(int));
        map_vals_out = (int *)malloc((size_t)hand.numbOfMapEntries * sizeof(int));

        /* Generate random keys and values for the map */
        for (i = 0; i < hand.numbOfMapEntries; i++) {
            map_keys[i] =
                (rand() % (256 * 256 * 256 * 32 / hand.numbOfMapEntries)) * hand.numbOfMapEntries + i;
            map_vals[i] = rand();
        } /* end for */
    }
    else if (!strcmp(hand.map_dtype, "vl")) {
        char key[NAME_LENGTH];

        vls_vl_keys = (char **)malloc((size_t)hand.numbOfMapEntries * sizeof(char *));
        vls_vl_vals = (hvl_t *)malloc((size_t)hand.numbOfMapEntries * sizeof(hvl_t));
        vls_vl_out  = (hvl_t *)malloc((size_t)hand.numbOfMapEntries * sizeof(hvl_t));

        /* Generate random keys and values for the map */
        for (i = 0; i < hand.numbOfMapEntries; i++) {
            sprintf(key, "map_vls_key_%d", i);
            vls_vl_keys[i] = strdup(key);

            vls_vl_vals[i].p   = malloc((size_t)(i + 4) * sizeof(int));
            vls_vl_vals[i].len = (size_t)i + 4;
            for (j = 0; j < (i + 4); j++)
                ((int *)vls_vl_vals[i].p)[j] = rand();
        } /* end for */
    }

    /* Allocate the memory for the attribute write and read */
    attr_write = (int *)malloc((size_t)hand.attr_dim * sizeof(int));
    attr_read  = (int *)malloc((size_t)hand.attr_dim * sizeof(int));

    /* Initialize the data for the attribute */
    for (i = 0; i < hand.attr_dim; i++)
        attr_write[i] = i;
}

static void
calculate_results()
{
    int     i, j;
    double *all_max_rate_buf, *all_min_rate_buf, *all_mean_rate_buf;
    int     total_nodes_per_tree = 0;
    double *rate_each_tree[ENTRY_NUM];
    double  total_rate[ENTRY_NUM];
    double  file_total_time[FILE_ENTRY_NUM];
    double  file_max_time[FILE_ENTRY_NUM], file_min_time[FILE_ENTRY_NUM];

    /* Calculate results of objects */
    for (i = 0; i < ENTRY_NUM; i++)
        rate_each_tree[i] = (double *)calloc((size_t)hand.numbOfTrees, sizeof(double));

    if (hand.depthOfTree == 0) {
        total_nodes_per_tree = 1;
    }
    else {
        for (i = 0; i <= hand.depthOfTree; i++)
            total_nodes_per_tree += (int)pow(hand.numbOfBranches, i);
    }

    memset(total_rate, 0, sizeof(double) * ENTRY_NUM);

    /* For independent I/O, each rank calcuclates its own rates (average, maximal, and minimal) among multiple
     * trees (iterations). For collective I/O, the rates are the final results. */
    for (i = 0; i < ENTRY_NUM; i++) {
        results.max_rate[i] = 0.0;
        results.min_rate[i] = 1000000000.0;

        for (j = 0; j < hand.numbOfTrees; j++) {
            /* For map put and get, multiply the number of map entries */
            if (i == MAP_PUT_NUM || i == MAP_GET_NUM)
                rate_each_tree[i][j] =
                    total_nodes_per_tree * hand.numbOfObjs * hand.numbOfMapEntries / op_time[i][j];
            else
                rate_each_tree[i][j] = total_nodes_per_tree * hand.numbOfObjs / op_time[i][j];

            total_rate[i] += rate_each_tree[i][j];

            if (rate_each_tree[i][j] > results.max_rate[i])
                results.max_rate[i] = rate_each_tree[i][j];
            if (rate_each_tree[i][j] < results.min_rate[i])
                results.min_rate[i] = rate_each_tree[i][j];
        }
        results.mean_rate[i] = total_rate[i] / hand.numbOfTrees;
    }

    if (hand.uniqueGroupPerRank) {
        all_max_rate_buf  = (double *)calloc((size_t)mpi_size, sizeof(double));
        all_min_rate_buf  = (double *)calloc((size_t)mpi_size, sizeof(double));
        all_mean_rate_buf = (double *)calloc((size_t)mpi_size, sizeof(double));
    }

    /* For independent I/O, gather the rate from all ranks and sum them up. */
    if (hand.uniqueGroupPerRank) {
        for (i = 0; i < ENTRY_NUM; i++) {
            MPI_Gather(&results.max_rate[i], 1, MPI_DOUBLE, all_max_rate_buf, 1, MPI_DOUBLE, 0,
                       MPI_COMM_WORLD);
            MPI_Gather(&results.min_rate[i], 1, MPI_DOUBLE, all_min_rate_buf, 1, MPI_DOUBLE, 0,
                       MPI_COMM_WORLD);
            MPI_Gather(&results.mean_rate[i], 1, MPI_DOUBLE, all_mean_rate_buf, 1, MPI_DOUBLE, 0,
                       MPI_COMM_WORLD);

            results.total_max_rate[i]  = 0;
            results.total_min_rate[i]  = 0;
            results.total_mean_rate[i] = 0;

            for (j = 0; j < mpi_size; j++) {
                results.total_max_rate[i] += all_max_rate_buf[j];
                results.total_min_rate[i] += all_min_rate_buf[j];
                results.total_mean_rate[i] += all_mean_rate_buf[j];
            }

            results.avg_max_rate[i]  = results.total_max_rate[i] / mpi_size;
            results.avg_min_rate[i]  = results.total_min_rate[i] / mpi_size;
            results.avg_mean_rate[i] = results.total_mean_rate[i] / mpi_size;

            memset(all_max_rate_buf, 0, (size_t)mpi_size * sizeof(double));
            memset(all_min_rate_buf, 0, (size_t)mpi_size * sizeof(double));
            memset(all_mean_rate_buf, 0, (size_t)mpi_size * sizeof(double));
        }
    }

    if (hand.uniqueGroupPerRank) {
        if (all_max_rate_buf)
            free(all_max_rate_buf);
        if (all_min_rate_buf)
            free(all_min_rate_buf);
        if (all_mean_rate_buf)
            free(all_mean_rate_buf);
    }

    /* Calculate results of files */
    memset(file_total_time, 0, sizeof(double) * FILE_ENTRY_NUM);
    memset(file_max_time, 0, sizeof(double) * FILE_ENTRY_NUM);
    memset(file_min_time, 1000000.0, sizeof(double) * FILE_ENTRY_NUM);

    /* For file operation, there should be no independent I/O */
    for (j = 0; j < FILE_ENTRY_NUM; j++) {
        for (i = 0; i < hand.numbOfFiles; i++) {
            file_total_time[j] += file_op_time[j][i];

            if (file_op_time[j][i] > file_max_time[j])
                file_max_time[j] = file_op_time[j][i];
            if (file_op_time[j][i] < file_min_time[j])
                file_min_time[j] = file_op_time[j][i];
        }

        results.file_mean_rate[j] = hand.numbOfFiles / file_total_time[j];
        results.file_max_rate[j]  = 1 / file_min_time[j];
        results.file_min_rate[j]  = 1 / file_max_time[j];
    }

    if (MAINPROCESS) {
        if (hand.uniqueGroupPerRank) {
            for (i = 0; i < ENTRY_NUM; i++) {
                printf("%s:		total max = %lf,	total min = %lf,	total mean = %lf\n", metadata_op[i],
                       results.total_max_rate[i], results.total_min_rate[i], results.total_mean_rate[i]);
                printf("				average max = %lf,	average min = %lf,	average mean = %lf\n\n",
                       results.avg_max_rate[i], results.avg_min_rate[i], results.avg_mean_rate[i]);
            }
        }
        else {
            for (i = 0; i < ENTRY_NUM; i++) {
                printf("%s:		total max = %lf,	total min = %lf,	total mean = %lf\n", metadata_op[i],
                       results.max_rate[i], results.min_rate[i], results.mean_rate[i]);
                printf("				average max = %lf,	average min = %lf,	average mean = %lf\n\n",
                       results.max_rate[i] / mpi_size, results.min_rate[i] / mpi_size,
                       results.mean_rate[i] / mpi_size);
            }
        }

        for (i = 0; i < FILE_ENTRY_NUM; i++)
            printf("%s:		max = %lf,	min = %lf,	mean = %lf\n", file_op[i], results.file_max_rate[i],
                   results.file_min_rate[i], results.file_mean_rate[i]);
    }

    for (i = 0; i < ENTRY_NUM; i++)
        free(rate_each_tree[i]);
}

static int
release_resources()
{
    int i;

    for (i = 0; i < ENTRY_NUM; i++)
        free(op_time[i]);

    for (i = 0; i < FILE_ENTRY_NUM; i++)
        free(file_op_time[i]);

    /* Free the memory buffers for the dataset */
    if (!strcmp(hand.dset_dtype, "int")) {
        if (wdata)
            free(wdata);
        if (rdata)
            free(rdata);
    }
    else if (!strcmp(hand.dset_dtype, "char")) {
        if (wdata_char)
            free(wdata_char);
        if (rdata_char)
            free(rdata_char);
    }
    else if (!strcmp(hand.dset_dtype, "llong")) {
        if (wdata_llong)
            free(wdata_llong);
        if (rdata_llong)
            free(rdata_llong);
    }
    else if (!strcmp(hand.dset_dtype, "float")) {
        if (wdata_float)
            free(wdata_float);
        if (rdata_float)
            free(rdata_float);
    }
    else if (!strcmp(hand.dset_dtype, "double")) {
        if (wdata_double)
            free(wdata_double);
        if (rdata_double)
            free(rdata_double);
    }

    /* Free the memory buffers for the map */
    if (!strcmp(hand.map_dtype, "int")) {
        if (map_keys)
            free(map_keys);
        if (map_vals)
            free(map_vals);
        if (map_vals_out)
            free(map_vals_out);
    }
    else if (!strcmp(hand.map_dtype, "vl")) {
        for (i = 0; i < hand.numbOfMapEntries; i++) {
            free(vls_vl_keys[i]);
            free(vls_vl_vals[i].p);
        }

        free(vls_vl_keys);
        free(vls_vl_vals);

        if (H5Tclose(map_vl_key_dtype_id) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close the VL string datatype for map key\n");
            goto error;
        }

        if (H5Tclose(map_vl_value_dtype_id) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close the VL datatype for map value\n");
            goto error;
        }
    }

    /* Free the memory buffers for the attribute */
    if (attr_write)
        free(attr_write);
    if (attr_read)
        free(attr_read);

    if (H5Sclose(file_dspace) < 0) {
        H5_FAILED();
        AT();
        printf("failed to close the file space\n");
        goto error;
    }

    if (!hand.uniqueGroupPerRank) {
        if (H5Sclose(file_dspace_select) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close the file space selection\n");
            goto error;
        }

        if (H5Sclose(mem_space) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close the memory data space\n");
            goto error;
        }
    }

    if (H5Pclose(dcpl_id) < 0) {
        H5_FAILED();
        AT();
        printf("failed to close the dataset creation property list\n");
        goto error;
    }

    if (H5Pclose(gcpl_id) < 0) {
        H5_FAILED();
        AT();
        printf("failed to close the group creation property list\n");
        goto error;
    }

    if (H5Pclose(gapl_id) < 0) {
        H5_FAILED();
        AT();
        printf("failed to close the group access property list\n");
        goto error;
    }

    if (H5Sclose(attr_space) < 0) {
        H5_FAILED();
        AT();
        printf("failed to close the attribute data space \n");
        goto error;
    }

    if (hand.dset_dtype)
        free(hand.dset_dtype);
    if (hand.dset_layout)
        free(hand.dset_layout);
    if (hand.map_dtype)
        free(hand.map_dtype);

    return 0;

error:
    return -1;
}

static int
create_ids()
{
    hsize_t dimsf[DSET_RANK];                    /* dataset dimensions */
    hsize_t start[DSET_RANK];                    /* for hyperslab setting */
    hsize_t count[DSET_RANK], stride[DSET_RANK]; /* for hyperslab setting */
    hsize_t chunk_dims[DSET_RANK];
    hsize_t attr_dim[ATTR_RANK];

    dimsf[0] = (hsize_t)hand.dset_dim1;
    dimsf[1] = (hsize_t)hand.dset_dim2;
    if ((file_dspace = H5Screate_simple(DSET_RANK, dimsf, NULL)) < 0) {
        H5_FAILED();
        AT();
        printf("failed to create the data space\n");
        goto error;
    }

    /* For collective I/O only, do hyperslab selection */
    if (!hand.uniqueGroupPerRank) {
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

        if ((mem_space = H5Screate_simple(DSET_RANK, count, NULL)) < 0) {
            H5_FAILED();
            AT();
            printf("failed to create memory space\n");
            goto error;
        }
    }

    /* Modify dataset creation properties, i.e. enable chunking  */
    if ((dcpl_id = H5Pcreate(H5P_DATASET_CREATE)) < 0) {
        H5_FAILED();
        AT();
        printf("failed to create dataset creation property list\n");
        goto error;
    }

    if (!strcmp(hand.dset_layout, "chunked")) {
        chunk_dims[0] = (hsize_t)hand.chunk_dim1;
        chunk_dims[1] = (hsize_t)hand.chunk_dim2;

        if (H5Pset_chunk(dcpl_id, DSET_RANK, chunk_dims) < 0) {
            H5_FAILED();
            AT();
            printf("failed to set chunk size to the dataset creation property list\n");
            goto error;
        }
    }
    else if (!strcmp(hand.dset_layout, "compact")) {
        if (H5Pset_layout(dcpl_id, H5D_COMPACT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to set layout to the dataset creation property list\n");
            goto error;
        }
    }
    else if (!strcmp(hand.dset_layout, "contiguous")) {
        if (H5Pset_layout(dcpl_id, H5D_CONTIGUOUS) < 0) {
            H5_FAILED();
            AT();
            printf("failed to set layout to the dataset creation property list\n");
            goto error;
        }
    }

    /* The S1 object class overwrites the default SX class for the datasets to improve performance */
    if (hand.uniqueGroupPerRank && H5daos_set_object_class(dcpl_id, "S1") < 0) {
        H5_FAILED();
        AT();
        printf("    couldn't set object class for dataset creation property list\n");
        goto error;
    }

    if (!strcmp(hand.map_dtype, "vl")) {
        if ((map_vl_key_dtype_id = H5Tcreate(H5T_STRING, H5T_VARIABLE)) < 0) {
            H5_FAILED();
            AT();
            printf("failed to create VL string datatype for map key\n");
            goto error;
        } /* end if */

        if ((map_vl_value_dtype_id = H5Tvlen_create(H5T_NATIVE_INT)) < 0) {
            H5_FAILED();
            AT();
            printf("failed to create VL datatype for map value\n");
            goto error;
        } /* end if */
    }

    /* Set object class for group creation property list */
    if ((gcpl_id = H5Pcreate(H5P_GROUP_CREATE)) < 0) {
        H5_FAILED();
        AT();
        printf("    couldn't create group creation property list\n");
        goto error;
    }

    /* The SX object class is only used for the unique group under which objects are tested */
    if (hand.uniqueGroupPerRank && H5daos_set_object_class(gcpl_id, "SX") < 0) {
        H5_FAILED();
        AT();
        printf("    couldn't set object class for group creation property list\n");
        goto error;
    }

    /* Enable the creation order tracking for the unique group under which objects are tested */
    if (hand.trackOrder &&
        H5Pset_link_creation_order(gcpl_id, H5P_CRT_ORDER_TRACKED | H5P_CRT_ORDER_INDEXED) < 0) {
        H5_FAILED();
        AT();
        printf("    couldn't set creation order for group creation property list\n");
        goto error;
    }

    /* The group access property list for create the tree nodes, which are supposed to be created collective.
     * This is a simple way to avoid creating the tree structure independently. */
    if ((gapl_id = H5Pcreate(H5P_GROUP_ACCESS)) < 0) {
        H5_FAILED();
        AT();
        printf("    couldn't create group creation property list\n");
        goto error;
    }

    if (H5Pset_all_coll_metadata_ops(gapl_id, TRUE) < 0) {
        H5_FAILED();
        AT();
        printf("    couldn't set collective for group access property list\n");
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

/* Callback function for H5Litereate2 */
static herr_t
link_iter_cb(hid_t group_id, const char *name, const H5L_info2_t *info, void *op_data)
{
    (void)group_id;
    (void)name;
    (void)info;
    (void)op_data;

    return 0;
}

static int
test_group(hid_t loc_id)
{
    hid_t     *gid, nested_gid;
    hbool_t   *exists_arr = NULL;
    char       gname[NAME_LENGTH], nested_gname[NAME_LENGTH];
    H5G_info_t group_info;
    hsize_t    op_data;
    int        i, j;
    double     start, end, time;
    hid_t      estack          = H5I_INVALID_HID;
    size_t     num_in_progress = 0;
    hbool_t    op_failed       = FALSE;

    if (hand.isAsync && (estack = H5EScreate()) < 0) {
        H5_FAILED();
        AT();
        printf("failed to create event set\n");
        goto error;
    }

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Create group objects */
    gid = (hid_t *)malloc((size_t)hand.numbOfObjs * sizeof(hid_t));

    start = MPI_Wtime();
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(gname, "group_object_%d", i + 1);

        if (hand.isAsync) {
            if ((gid[i] = H5Gcreate_async(loc_id, gname, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT, estack)) <
                0) {
                H5_FAILED();
                AT();
                printf("failed to create group object '%s'\n", gname);
                goto error;
            }

            if (hand.useWait && (i % hand.waitInterval == 0)) {
                if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed ||
                    num_in_progress) {
                    H5_FAILED();
                    AT();
                    printf("failed to wait for group creation\n");
                    goto error;
                }
            }
        }
        else {
            if ((gid[i] = H5Gcreate2(loc_id, gname, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
                H5_FAILED();
                AT();
                printf("failed to create group object '%s'\n", gname);
                goto error;
            }
        }

        /* Create some subgroups */
        if (!hand.noGroupMember) {
            for (j = 0; j < hand.numbOfNestedGroups; j++) {
                sprintf(nested_gname, "nested_group_%d", j + 1);

                if (hand.isAsync) {
                    if ((nested_gid = H5Gcreate_async(gid[i], nested_gname, H5P_DEFAULT, H5P_DEFAULT,
                                                      H5P_DEFAULT, estack)) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to create group object '%s'\n", nested_gname);
                        goto error;
                    }
                }
                else {
                    if ((nested_gid =
                             H5Gcreate2(gid[i], nested_gname, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to create group object '%s'\n", nested_gname);
                        goto error;
                    }
                }

                if (hand.isAsync) {
                    if (H5Gclose_async(nested_gid, estack) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to close the group '%s'\n", nested_gname);
                        goto error;
                    }
                }
                else {
                    if (H5Gclose(nested_gid) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to close the group '%s'\n", nested_gname);
                        goto error;
                    }
                }
            }
        }
    }

    if (hand.isAsync) {
        if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed || num_in_progress) {
            H5_FAILED();
            AT();
            printf("failed to wait for group creation\n");
            goto error;
        }
    }

    end  = MPI_Wtime();
    time = end - start;
    op_time[GROUP_CREATE_NUM][tree_order] += time;

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Group close */
    start = MPI_Wtime();
    for (i = 0; i < hand.numbOfObjs; i++) {
        if (hand.isAsync) {
            if (H5Gclose_async(gid[i], estack) < 0) {
                H5_FAILED();
                AT();
                printf("failed to close the group '%s'\n", gname);
                goto error;
            }
        }
        else {
            if (H5Gclose(gid[i]) < 0) {
                H5_FAILED();
                AT();
                printf("failed to close the group '%s'\n", gname);
                goto error;
            }
        }
    }

    if (hand.isAsync) {
        if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed || num_in_progress) {
            H5_FAILED();
            AT();
            printf("failed to wait for group close\n");
            goto error;
        }
    }

    end  = MPI_Wtime();
    time = end - start;
    op_time[GROUP_CLOSE_NUM][tree_order] += time;

    if (hand.isAsync && H5Oflush_async(loc_id, estack) < 0) {
        H5_FAILED();
        AT();
        printf("failed to flush group object\n");
        goto error;
    }

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Group open */
    start = MPI_Wtime();
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(gname, "group_object_%d", i + 1);

        if (hand.isAsync) {
            if ((gid[i] = H5Gopen_async(loc_id, gname, H5P_DEFAULT, estack)) < 0) {
                H5_FAILED();
                AT();
                printf("failed to open the group '%s'\n", gname);
                goto error;
            }

            if (hand.useWait && (i % hand.waitInterval == 0)) {
                if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed ||
                    num_in_progress) {
                    H5_FAILED();
                    AT();
                    printf("failed to wait for group creation\n");
                    goto error;
                }
            }
        }
        else {
            if ((gid[i] = H5Gopen2(loc_id, gname, H5P_DEFAULT)) < 0) {
                H5_FAILED();
                AT();
                printf("failed to open the group '%s'\n", gname);
                goto error;
            }
        }
    }

    if (hand.isAsync) {
        if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed || num_in_progress) {
            H5_FAILED();
            AT();
            printf("failed to wait for group open\n");
            goto error;
        }
    }

    end  = MPI_Wtime();
    time = end - start;
    op_time[GROUP_OPEN_NUM][tree_order] += time;

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Group info */
    start = MPI_Wtime();
    for (i = 0; i < hand.numbOfObjs; i++) {
        if (hand.isAsync) {
            if (H5Gget_info_async(gid[i], &group_info, estack) < 0) {
                H5_FAILED();
                AT();
                printf("failed to get info for the group '%s'\n", gname);
                goto error;
            }
        }
        else {
            if (H5Gget_info(gid[i], &group_info) < 0) {
                H5_FAILED();
                AT();
                printf("failed to get info for the group '%s'\n", gname);
                goto error;
            }
        }
    }

    if (hand.isAsync) {
        if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed || num_in_progress) {
            H5_FAILED();
            AT();
            printf("failed to wait for group info\n");
            goto error;
        }
    }

    end  = MPI_Wtime();
    time = end - start;
    op_time[GROUP_INFO_NUM][tree_order] += time;

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Group close */
    for (i = 0; i < hand.numbOfObjs; i++) {
        if (hand.isAsync) {
            if (H5Gclose_async(gid[i], estack) < 0) {
                H5_FAILED();
                AT();
                printf("failed to close the group '%s'\n", gname);
                goto error;
            }
        }
        else {
            if (H5Gclose(gid[i]) < 0) {
                H5_FAILED();
                AT();
                printf("failed to close the group '%s'\n", gname);
                goto error;
            }
        }
    }

    if (hand.isAsync) {
        if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed || num_in_progress) {
            H5_FAILED();
            AT();
            printf("failed to wait for group close\n");
            goto error;
        }
    }

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Link traversal */
    {
        op_data = 0;
        start   = MPI_Wtime();

        if (H5Literate2(loc_id, H5_INDEX_NAME, H5_ITER_INC, NULL, link_iter_cb, &op_data) < 0) {
            H5_FAILED();
            AT();
            printf("failed to traverse the links\n");
            goto error;
        }

        end  = MPI_Wtime();
        time = end - start;
        op_time[LINK_ITERATE_NUM][tree_order] += time;

#ifdef DEBUG
        printf("Link iterate time: %lf, mpi_rank=%d\n", time, mpi_rank);
#endif
    }

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Allocate array of existence results */
    if (hand.isAsync)
        if (NULL == (exists_arr = (hbool_t *)malloc((size_t)hand.numbOfObjs * sizeof(hbool_t)))) {
            H5_FAILED();
            AT();
            printf("failed allocate array of link existence check results\n");
            goto error;
        }

    /* Link existence and group open */
    for (i = 0; i < hand.numbOfObjs; i++) {
        htri_t exists;

        sprintf(gname, "group_object_%d", i + 1);

        start = MPI_Wtime();

        if (hand.isAsync) {
            if (H5Lexists_async(loc_id, gname, &exists_arr[i], H5P_DEFAULT, estack) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check the existence of the group '%s'\n", gname);
                goto error;
            }
        }
        else {
            if ((exists = H5Lexists(loc_id, gname, H5P_DEFAULT)) < 0) {
                H5_FAILED();
                AT();
                printf("failed to check the existence of the group '%s'\n", gname);
                goto error;
            }
            if (!exists) {
                H5_FAILED();
                AT();
                printf("group '%s' does not exist\n", gname);
                goto error;
            }
        }

        end  = MPI_Wtime();
        time = end - start;
        op_time[LINK_EXIST_NUM][tree_order] += time;

#ifdef DEBUG
        printf("Link exist time: %lf, mpi_rank=%d\n", time, mpi_rank);
#endif
    }

    /* Wait and verify existence results */
    if (hand.isAsync) {
        if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed || num_in_progress) {
            H5_FAILED();
            AT();
            printf("failed to wait for group removal\n");
            goto error;
        }
        for (i = 0; i < hand.numbOfObjs; i++)
            if (!exists_arr[i]) {
                H5_FAILED();
                AT();
                printf("group %d does not exist\n", i + 1);
                goto error;
            }
    }

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Group removal */
    start = MPI_Wtime();
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(gname, "group_object_%d", i + 1);

        if (hand.isAsync) {
            if (H5Ldelete_async(loc_id, gname, H5P_DEFAULT, estack) < 0) {
                H5_FAILED();
                AT();
                printf("failed to remove the group '%s'\n", gname);
                goto error;
            }
        }
        else {
            if (H5Ldelete(loc_id, gname, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to remove the group '%s'\n", gname);
                goto error;
            }
        }
    }

    if (hand.isAsync) {
        if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed || num_in_progress) {
            H5_FAILED();
            AT();
            printf("failed to wait for group removal\n");
            goto error;
        }
    }

    end  = MPI_Wtime();
    time = end - start;
    op_time[GROUP_REMOVE_NUM][tree_order] += time;

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    if (hand.isAsync && H5ESclose(estack) < 0) {
        H5_FAILED();
        AT();
        printf("failed to close event set\n");
        goto error;
    }

    if (gid)
        free(gid);

    if (exists_arr)
        free(exists_arr);

    return 0;

error:
    if (gid)
        free(gid);

    if (exists_arr)
        free(exists_arr);

    if (hand.isAsync && H5ESclose(estack) < 0) {
        H5_FAILED();
        AT();
        printf("failed to close event set\n");
        goto error;
    }

    return -1;
}

static int
test_dataset(hid_t loc_id)
{
    hid_t      *dset_id;
    char        dset_name[NAME_LENGTH];
    H5O_info2_t obj_info;
    int         i;
    double      start, end, time;
    hid_t       estack          = H5I_INVALID_HID;
    size_t      num_in_progress = 0;
    hbool_t     op_failed       = FALSE;

    if (hand.isAsync && (estack = H5EScreate()) < 0) {
        H5_FAILED();
        AT();
        printf("failed to create event set\n");
        goto error;
    }

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Dataset creation */
    dset_id = (hid_t *)malloc((size_t)hand.numbOfObjs * sizeof(hid_t));

    start = MPI_Wtime();
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(dset_name, "dset_object_%d", i + 1);

        if (hand.isAsync) {
            if ((dset_id[i] = H5Dcreate_async(loc_id, dset_name, H5T_NATIVE_INT, file_dspace, H5P_DEFAULT,
                                              dcpl_id, H5P_DEFAULT, estack)) < 0) {
                H5_FAILED();
                AT();
                printf("failed to create dataset object '%s'\n", dset_name);
                goto error;
            }

            if (hand.useWait && (i % hand.waitInterval == 0)) {
                if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed ||
                    num_in_progress) {
                    H5_FAILED();
                    AT();
                    printf("failed to wait for dataset creation\n");
                    goto error;
                }
            }
        }
        else {
            if ((dset_id[i] = H5Dcreate2(loc_id, dset_name, H5T_NATIVE_INT, file_dspace, H5P_DEFAULT, dcpl_id,
                                         H5P_DEFAULT)) < 0) {
                H5_FAILED();
                AT();
                printf("failed to create dataset object '%s'\n", dset_name);
                goto error;
            }
        }
    }

    if (hand.isAsync) {
        if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed || num_in_progress) {
            H5_FAILED();
            AT();
            printf("failed to wait for dataset creation\n");
            goto error;
        }
    }

    end  = MPI_Wtime();
    time = end - start;
    op_time[DSET_CREATE_NUM][tree_order] += time;
    op_time[DSET_CREATEWRITECLOSE_NUM][tree_order] += time;

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Dataset write */
    if (!hand.noWriteReadData) {
        start = MPI_Wtime();

        for (i = 0; i < hand.numbOfObjs; i++) {
            if (!hand.uniqueGroupPerRank) {
                if (!strcmp(hand.dset_dtype, "int")) {
                    if (H5Dwrite(dset_id[i], H5T_NATIVE_INT, mem_space, file_dspace_select, H5P_DEFAULT,
                                 wdata) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to write the dataset '%s'\n", dset_name);
                        goto error;
                    }
                }
                else if (!strcmp(hand.dset_dtype, "char")) {
                    if (H5Dwrite(dset_id[i], H5T_NATIVE_CHAR, mem_space, file_dspace_select, H5P_DEFAULT,
                                 wdata_char) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to write the dataset '%s'\n", dset_name);
                        goto error;
                    }
                }
                else if (!strcmp(hand.dset_dtype, "llong")) {
                    if (H5Dwrite(dset_id[i], H5T_NATIVE_LLONG, mem_space, file_dspace_select, H5P_DEFAULT,
                                 wdata_llong) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to write the dataset '%s'\n", dset_name);
                        goto error;
                    }
                }
                else if (!strcmp(hand.dset_dtype, "float")) {
                    if (H5Dwrite(dset_id[i], H5T_NATIVE_FLOAT, mem_space, file_dspace_select, H5P_DEFAULT,
                                 wdata_float) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to write the dataset '%s'\n", dset_name);
                        goto error;
                    }
                }
                else if (!strcmp(hand.dset_dtype, "double")) {
                    if (H5Dwrite(dset_id[i], H5T_NATIVE_DOUBLE, mem_space, file_dspace_select, H5P_DEFAULT,
                                 wdata_double) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to write the dataset '%s'\n", dset_name);
                        goto error;
                    }
                }
            }
            else {
                if (!strcmp(hand.dset_dtype, "int")) {
                    if (hand.isAsync && H5Dwrite_async(dset_id[i], H5T_NATIVE_INT, file_dspace, file_dspace,
                                                       H5P_DEFAULT, wdata, estack) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to write the dataset '%s'\n", dset_name);
                        goto error;
                    }
                    else if (H5Dwrite(dset_id[i], H5T_NATIVE_INT, file_dspace, file_dspace, H5P_DEFAULT,
                                      wdata) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to write the dataset '%s'\n", dset_name);
                        goto error;
                    }
                }
                else if (!strcmp(hand.dset_dtype, "char")) {
                    if (hand.isAsync && H5Dwrite_async(dset_id[i], H5T_NATIVE_CHAR, file_dspace, file_dspace,
                                                       H5P_DEFAULT, wdata_char, estack) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to write the dataset '%s'\n", dset_name);
                        goto error;
                    }
                    else if (H5Dwrite(dset_id[i], H5T_NATIVE_CHAR, file_dspace, file_dspace, H5P_DEFAULT,
                                      wdata_char) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to write the dataset '%s'\n", dset_name);
                        goto error;
                    }
                }
                else if (!strcmp(hand.dset_dtype, "llong")) {
                    if (hand.isAsync && H5Dwrite_async(dset_id[i], H5T_NATIVE_LLONG, file_dspace, file_dspace,
                                                       H5P_DEFAULT, wdata_llong, estack) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to write the dataset '%s'\n", dset_name);
                        goto error;
                    }
                    else if (H5Dwrite(dset_id[i], H5T_NATIVE_LLONG, file_dspace, file_dspace, H5P_DEFAULT,
                                      wdata_llong) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to write the dataset '%s'\n", dset_name);
                        goto error;
                    }
                }
                else if (!strcmp(hand.dset_dtype, "float")) {
                    if (hand.isAsync && H5Dwrite_async(dset_id[i], H5T_NATIVE_FLOAT, file_dspace, file_dspace,
                                                       H5P_DEFAULT, wdata_float, estack) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to write the dataset '%s'\n", dset_name);
                        goto error;
                    }
                    else if (H5Dwrite(dset_id[i], H5T_NATIVE_FLOAT, file_dspace, file_dspace, H5P_DEFAULT,
                                      wdata_float) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to write the dataset '%s'\n", dset_name);
                        goto error;
                    }
                }
                else if (!strcmp(hand.dset_dtype, "double")) {
                    if (hand.isAsync && H5Dwrite_async(dset_id[i], H5T_NATIVE_DOUBLE, file_dspace,
                                                       file_dspace, H5P_DEFAULT, wdata_double, estack) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to write the dataset '%s'\n", dset_name);
                        goto error;
                    }
                    else if (H5Dwrite(dset_id[i], H5T_NATIVE_DOUBLE, file_dspace, file_dspace, H5P_DEFAULT,
                                      wdata_double) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to write the dataset '%s'\n", dset_name);
                        goto error;
                    }
                }
            }
        }

        if (hand.isAsync) {
            if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed ||
                num_in_progress) {
                H5_FAILED();
                AT();
                printf("failed to wait for dataset write\n");
                goto error;
            }
        }

        end  = MPI_Wtime();
        time = end - start;
        op_time[DSET_WRITE_NUM][tree_order] += time;
        op_time[DSET_CREATEWRITECLOSE_NUM][tree_order] += time;
    }

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Dataset close */
    start = MPI_Wtime();
    for (i = 0; i < hand.numbOfObjs; i++) {
        if (hand.isAsync) {
            if (H5Dclose_async(dset_id[i], estack) < 0) {
                H5_FAILED();
                AT();
                printf("failed to close dataset object\n");
                goto error;
            }
        }
        else {
            if (H5Dclose(dset_id[i]) < 0) {
                H5_FAILED();
                AT();
                printf("failed to close dataset object\n");
                goto error;
            }
        }
    }

    if (hand.isAsync) {
        if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed || num_in_progress) {
            H5_FAILED();
            AT();
            printf("failed to wait for dataset close\n");
            goto error;
        }
    }

    end  = MPI_Wtime();
    time = end - start;
    op_time[DSET_CLOSE_NUM][tree_order] += time;
    op_time[DSET_CREATEWRITECLOSE_NUM][tree_order] += time;

    if (hand.isAsync && H5Oflush_async(loc_id, estack) < 0) {
        H5_FAILED();
        AT();
        printf("failed to flush dataset object\n");
        goto error;
    }

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Dataset open */
    start = MPI_Wtime();
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(dset_name, "dset_object_%d", i + 1);

        if (hand.isAsync) {
            if ((dset_id[i] = H5Dopen_async(loc_id, dset_name, H5P_DEFAULT, estack)) < 0) {
                H5_FAILED();
                AT();
                printf("failed to open dataset object '%s'\n", dset_name);
                goto error;
            }

            if (hand.useWait && (i % hand.waitInterval == 0)) {
                if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed ||
                    num_in_progress) {
                    H5_FAILED();
                    AT();
                    printf("failed to wait for group creation\n");
                    goto error;
                }
            }
        }
        else {
            if ((dset_id[i] = H5Dopen2(loc_id, dset_name, H5P_DEFAULT)) < 0) {
                H5_FAILED();
                AT();
                printf("failed to open dataset object '%s'\n", dset_name);
                goto error;
            }
        }
    }

    if (hand.isAsync) {
        if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed || num_in_progress) {
            H5_FAILED();
            AT();
            printf("failed to wait for dataset open\n");
            goto error;
        }
    }

    end  = MPI_Wtime();
    time = end - start;
    op_time[DSET_OPEN_NUM][tree_order] += time;
    op_time[DSET_OPENREADCLOSE_NUM][tree_order] += time;

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Dataset read */
    if (!hand.noWriteReadData) {
        start = MPI_Wtime();

        for (i = 0; i < hand.numbOfObjs; i++) {
            if (!hand.uniqueGroupPerRank) {
                if (!strcmp(hand.dset_dtype, "int")) {
                    if (H5Dread(dset_id[i], H5T_NATIVE_INT, mem_space, file_dspace_select, H5P_DEFAULT,
                                rdata) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to read the dataset '%s'\n", dset_name);
                        goto error;
                    }
                }
                else if (!strcmp(hand.dset_dtype, "char")) {
                    if (H5Dread(dset_id[i], H5T_NATIVE_CHAR, mem_space, file_dspace_select, H5P_DEFAULT,
                                rdata_char) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to read the dataset '%s'\n", dset_name);
                        goto error;
                    }
                }
                else if (!strcmp(hand.dset_dtype, "llong")) {
                    if (H5Dread(dset_id[i], H5T_NATIVE_LLONG, mem_space, file_dspace_select, H5P_DEFAULT,
                                rdata_llong) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to read the dataset '%s'\n", dset_name);
                        goto error;
                    }
                }
                else if (!strcmp(hand.dset_dtype, "float")) {
                    if (H5Dread(dset_id[i], H5T_NATIVE_FLOAT, mem_space, file_dspace_select, H5P_DEFAULT,
                                rdata_float) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to read the dataset '%s'\n", dset_name);
                        goto error;
                    }
                }
                else if (!strcmp(hand.dset_dtype, "double")) {
                    if (H5Dread(dset_id[i], H5T_NATIVE_DOUBLE, mem_space, file_dspace_select, H5P_DEFAULT,
                                rdata_double) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to read the dataset '%s'\n", dset_name);
                        goto error;
                    }
                }
            }
            else {
                if (!strcmp(hand.dset_dtype, "int")) {
                    if (hand.isAsync && H5Dread_async(dset_id[i], H5T_NATIVE_INT, file_dspace, file_dspace,
                                                      H5P_DEFAULT, rdata, estack) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to read the dataset '%s'\n", dset_name);
                        goto error;
                    }
                    else if (H5Dread(dset_id[i], H5T_NATIVE_INT, file_dspace, file_dspace, H5P_DEFAULT,
                                     rdata) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to read the dataset '%s'\n", dset_name);
                        goto error;
                    }
                }
                else if (!strcmp(hand.dset_dtype, "char")) {
                    if (hand.isAsync && H5Dread_async(dset_id[i], H5T_NATIVE_CHAR, file_dspace, file_dspace,
                                                      H5P_DEFAULT, rdata_char, estack) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to read the dataset '%s'\n", dset_name);
                        goto error;
                    }
                    else if (H5Dread(dset_id[i], H5T_NATIVE_CHAR, file_dspace, file_dspace, H5P_DEFAULT,
                                     rdata_char) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to read the dataset '%s'\n", dset_name);
                        goto error;
                    }
                }
                else if (!strcmp(hand.dset_dtype, "llong")) {
                    if (hand.isAsync && H5Dread_async(dset_id[i], H5T_NATIVE_LLONG, file_dspace, file_dspace,
                                                      H5P_DEFAULT, rdata_llong, estack) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to read the dataset '%s'\n", dset_name);
                        goto error;
                    }
                    else if (H5Dread(dset_id[i], H5T_NATIVE_LLONG, file_dspace, file_dspace, H5P_DEFAULT,
                                     rdata_llong) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to read the dataset '%s'\n", dset_name);
                        goto error;
                    }
                }
                else if (!strcmp(hand.dset_dtype, "float")) {
                    if (hand.isAsync && H5Dread_async(dset_id[i], H5T_NATIVE_FLOAT, file_dspace, file_dspace,
                                                      H5P_DEFAULT, rdata_float, estack) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to read the dataset '%s'\n", dset_name);
                        goto error;
                    }
                    else if (H5Dread(dset_id[i], H5T_NATIVE_FLOAT, file_dspace, file_dspace, H5P_DEFAULT,
                                     rdata_float) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to read the dataset '%s'\n", dset_name);
                        goto error;
                    }
                }
                else if (!strcmp(hand.dset_dtype, "double")) {
                    if (hand.isAsync && H5Dread_async(dset_id[i], H5T_NATIVE_DOUBLE, file_dspace, file_dspace,
                                                      H5P_DEFAULT, rdata_double, estack) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to read the dataset '%s'\n", dset_name);
                        goto error;
                    }
                    else if (H5Dread(dset_id[i], H5T_NATIVE_DOUBLE, file_dspace, file_dspace, H5P_DEFAULT,
                                     rdata_double) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to read the dataset '%s'\n", dset_name);
                        goto error;
                    }
                }
            }
        }
    }

    if (hand.isAsync) {
        if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed || num_in_progress) {
            H5_FAILED();
            AT();
            printf("failed to wait for dataset read\n");
            goto error;
        }
    }

    end  = MPI_Wtime();
    time = end - start;
    op_time[DSET_READ_NUM][tree_order] += time;
    op_time[DSET_OPENREADCLOSE_NUM][tree_order] += time;

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Dataset info - no async function available */
    start = MPI_Wtime();
    for (i = 0; i < hand.numbOfObjs; i++) {
        if (H5Oget_info3(dset_id[i], &obj_info, H5O_INFO_BASIC) < 0) {
            H5_FAILED();
            AT();
            printf("failed to query dataset object\n");
            goto error;
        }
    }

    end  = MPI_Wtime();
    time = end - start;
    op_time[DSET_INFO_NUM][tree_order] += time;

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Dataset close */
    start = MPI_Wtime();
    for (i = 0; i < hand.numbOfObjs; i++) {
        if (hand.isAsync) {
            if (H5Dclose_async(dset_id[i], estack) < 0) {
                H5_FAILED();
                AT();
                printf("failed to close dataset object\n");
                goto error;
            }
        }
        else {
            if (H5Dclose(dset_id[i]) < 0) {
                H5_FAILED();
                AT();
                printf("failed to close dataset object\n");
                goto error;
            }
        }
    }

    if (hand.isAsync) {
        if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed || num_in_progress) {
            H5_FAILED();
            AT();
            printf("failed to wait for dataset close\n");
            goto error;
        }
    }

    end  = MPI_Wtime();
    time = end - start;
    op_time[DSET_OPENREADCLOSE_NUM][tree_order] += time;

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Dataset removal */
    start = MPI_Wtime();
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(dset_name, "dset_object_%d", i + 1);

        if (hand.isAsync) {
            if (H5Ldelete_async(loc_id, dset_name, H5P_DEFAULT, estack) < 0) {
                H5_FAILED();
                AT();
                printf("failed to remove the dataset '%s'\n", dset_name);
                goto error;
            }
        }
        else {
            if (H5Ldelete(loc_id, dset_name, H5P_DEFAULT) < 0) {
                H5_FAILED();
                AT();
                printf("failed to remove the dataset '%s'\n", dset_name);
                goto error;
            }
        }
    }

    if (hand.isAsync) {
        if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed || num_in_progress) {
            H5_FAILED();
            AT();
            printf("failed to wait for dataset close\n");
            goto error;
        }
    }

    end  = MPI_Wtime();
    time = end - start;
    op_time[DSET_REMOVE_NUM][tree_order] += time;

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    if (dset_id)
        free(dset_id);

    if (hand.isAsync && H5ESclose(estack) < 0) {
        H5_FAILED();
        AT();
        printf("failed to close event set\n");
        goto error;
    }

    return 0;

error:
    if (dset_id)
        free(dset_id);

    if (hand.isAsync && H5ESclose(estack) < 0) {
        H5_FAILED();
        AT();
        printf("failed to close event set\n");
        goto error;
    }

    return -1;
}

static int
test_attribute(hid_t loc_id)
{
    hid_t  attr_id = H5I_INVALID_HID;
    char   attr_name[NAME_LENGTH];
    int    i;
    double start, end, time;

    /* Attribute creation */
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(attr_name, "attribute_object_%d", i + 1);
        start = MPI_Wtime();

        if ((attr_id = H5Acreate2(loc_id, attr_name, H5T_NATIVE_INT, attr_space, H5P_DEFAULT, H5P_DEFAULT)) <
            0) {
            H5_FAILED();
            AT();
            printf("failed to create attribute object '%s'\n", attr_name);
            goto error;
        }

        end  = MPI_Wtime();
        time = end - start;
        op_time[ATTR_CREATE_NUM][tree_order] += time;

#ifdef DEBUG
        printf("Attr creation time: %lf, mpi_rank=%d\n", time, mpi_rank);
#endif

        start = MPI_Wtime();

        if (H5Awrite(attr_id, H5T_NATIVE_INT, attr_write) < 0) {
            H5_FAILED();
            AT();
            printf("failed to write data to the attribute: %s\n", attr_name);
            goto error;
        }

        end  = MPI_Wtime();
        time = end - start;
        op_time[ATTR_WRITE_NUM][tree_order] += time;

#ifdef DEBUG
        printf("Attr write time: %lf, mpi_rank=%d\n", time, mpi_rank);
#endif

        start = MPI_Wtime();

        if (H5Aclose(attr_id) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close the attribute\n");
            goto error;
        }

        end  = MPI_Wtime();
        time = end - start;
        op_time[ATTR_CLOSE_NUM][tree_order] += time;

#ifdef DEBUG
        printf("Attr close time: %lf, mpi_rank=%d\n", time, mpi_rank);
#endif
    }

    /* Attribute open */
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(attr_name, "attribute_object_%d", i + 1);
        start = MPI_Wtime();

        if ((attr_id = H5Aopen(loc_id, attr_name, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            AT();
            printf("failed to open attribute object '%s'\n", attr_name);
            goto error;
        }

        end  = MPI_Wtime();
        time = end - start;
        op_time[ATTR_OPEN_NUM][tree_order] += time;

#ifdef DEBUG
        printf("Attr open time: %lf, mpi_rank=%d\n", time, mpi_rank);
#endif
        start = MPI_Wtime();

        if (H5Aread(attr_id, H5T_NATIVE_INT, attr_read) < 0) {
            H5_FAILED();
            AT();
            printf("failed to read data to the attribute: %s\n", attr_name);
            goto error;
        }

        end  = MPI_Wtime();
        time = end - start;
        op_time[ATTR_READ_NUM][tree_order] += time;

#ifdef DEBUG
        printf("Attr read time: %lf, mpi_rank=%d\n", time, mpi_rank);
#endif

        start = MPI_Wtime();

        if (H5Aclose(attr_id) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close the attribute\n");
            goto error;
        }
    }

    /* Attribute removal */
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(attr_name, "attribute_object_%d", i + 1);
        start = MPI_Wtime();

        if (H5Adelete(loc_id, attr_name) < 0) {
            H5_FAILED();
            AT();
            printf("failed to remove the attribute '%s'\n", attr_name);
            goto error;
        }

        end  = MPI_Wtime();
        time = end - start;
        op_time[ATTR_REMOVE_NUM][tree_order] += time;

#ifdef DEBUG
        printf("Attr removal time: %lf, mpi_rank=%d\n", time, mpi_rank);
#endif
    }

    return 0;

error:
    return -1;
}

static int
test_datatype(hid_t loc_id)
{
    hid_t  dtype_id = H5I_INVALID_HID;
    char   dtype_name[NAME_LENGTH];
    int    i;
    double start, end, time;

    /* Datatype commit */
    for (i = 0; i < hand.numbOfObjs; i++) {
        if ((dtype_id = H5Tcopy(H5T_NATIVE_INT)) < 0) {
            H5_FAILED();
            AT();
            printf("failed to copy a datatype\n");
            goto error;
        }

        sprintf(dtype_name, "dtype_object_%d", i + 1);
        start = MPI_Wtime();

        if (H5Tcommit2(loc_id, dtype_name, dtype_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to commit datatype object '%s'\n", dtype_name);
            goto error;
        }

        end  = MPI_Wtime();
        time = end - start;
        op_time[DTYPE_COMMIT_NUM][tree_order] += time;

#ifdef DEBUG
        printf("Dtype commit time: %lf, mpi_rank=%d\n", time, mpi_rank);
#endif

        start = MPI_Wtime();

        if (H5Tclose(dtype_id) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close the datatype\n");
            goto error;
        }

        end  = MPI_Wtime();
        time = end - start;
        op_time[DTYPE_CLOSE_NUM][tree_order] += time;

#ifdef DEBUG
        printf("Dtype close time: %lf, mpi_rank=%d\n", time, mpi_rank);
#endif
    }

    /* Datatype open */
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(dtype_name, "dtype_object_%d", i + 1);
        start = MPI_Wtime();

        if ((dtype_id = H5Topen(loc_id, dtype_name, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            AT();
            printf("failed to open datatype object '%s'\n", dtype_name);
            goto error;
        }

        end  = MPI_Wtime();
        time = end - start;
        op_time[DTYPE_OPEN_NUM][tree_order] += time;

#ifdef DEBUG
        printf("Dtype open time: %lf, mpi_rank=%d\n", time, mpi_rank);
#endif

        if (H5Tclose(dtype_id) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close the datatype\n");
            goto error;
        }
    }

    return 0;

error:
    return -1;
}

static int
test_H5O(hid_t loc_id)
{
    hid_t  gid = H5I_INVALID_HID, nested_gid = H5I_INVALID_HID;
    char   gname[NAME_LENGTH], nested_gname[NAME_LENGTH], obj_cp_name[NAME_LENGTH];
    int    i, j;
    double start, end, time;

    /* Create objects to copy */
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(gname, "H5O_group_object_%d", i + 1);

        if ((gid = H5Gcreate2(loc_id, gname, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
            H5_FAILED();
            AT();
            printf("failed to create group object '%s'\n", gname);
            goto error;
        }

        /* Create some subgroups */
        if (!hand.noGroupMember) {
            for (j = 0; j < hand.numbOfNestedGroups; j++) {
                sprintf(nested_gname, "nested_group_%d", j + 1);

                if ((nested_gid = H5Gcreate2(gid, nested_gname, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
                    H5_FAILED();
                    AT();
                    printf("failed to create group object '%s'\n", nested_gname);
                    goto error;
                }

                if (H5Gclose(nested_gid) < 0) {
                    H5_FAILED();
                    AT();
                    printf("failed to close the group '%s'\n", nested_gname);
                    goto error;
                }
            }
        }

        if (H5Gclose(gid) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close the group '%s'\n", gname);
            goto error;
        }
    }

    /* Object open for groups */
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(gname, "H5O_group_object_%d", i + 1);

        start = MPI_Wtime();

        if (H5Oopen(loc_id, gname, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to open group '%s'\n", gname);
            goto error;
        }

        end  = MPI_Wtime();
        time = end - start;
        op_time[OBJ_OPEN_GRP_NUM][tree_order] += time;

#ifdef DEBUG
        printf("H5Oopen group time: %lf, mpi_rank=%d\n", time, mpi_rank);
#endif
    }

    /* Object copy for groups */
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(gname, "H5O_group_object_%d", i + 1);
        sprintf(obj_cp_name, "H5O_group_object_copy_%d", i + 1);

        start = MPI_Wtime();
        if (H5Ocopy(loc_id, gname, loc_id, obj_cp_name, H5P_DEFAULT, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to copy the group '%s' to '%s'\n", gname, obj_cp_name);
            goto error;
        }

        end  = MPI_Wtime();
        time = end - start;
        op_time[OBJ_COPY_GRP_NUM][tree_order] += time;

#ifdef DEBUG
        printf("H5Ocopy group time: %lf, mpi_rank=%d\n", time, mpi_rank);
#endif
    }

    return 0;

error:
    return -1;
}

static int
test_map(hid_t loc_id)
{
    hid_t  *map_id = NULL;
    char    map_name[NAME_LENGTH];
    int     i, j;
    double  start, end, time;
    hid_t   estack          = H5I_INVALID_HID;
    size_t  num_in_progress = 0;
    hbool_t op_failed       = FALSE;

    if (hand.isAsync && (estack = H5EScreate()) < 0) {
        H5_FAILED();
        AT();
        printf("failed to create event set\n");
        goto error;
    }

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Create map */
    map_id = (hid_t *)malloc((size_t)hand.numbOfObjs * sizeof(hid_t));

    start = MPI_Wtime();
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(map_name, "map_object_%d", i + 1);
        if (!strcmp(hand.map_dtype, "int")) {
            if (hand.isAsync) {
                if ((map_id[i] = H5Mcreate_async(loc_id, map_name, H5T_NATIVE_INT, H5T_NATIVE_INT,
                                                 H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT, estack)) < 0) {
                    H5_FAILED();
                    AT();
                    printf("failed to create map object '%s'\n", map_name);
                    goto error;
                }

                if (hand.useWait && (i % hand.waitInterval == 0)) {
                    if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed ||
                        num_in_progress) {
                        H5_FAILED();
                        AT();
                        printf("failed to wait for map creation\n");
                        goto error;
                    }
                }
            }
            else {
                if ((map_id[i] = H5Mcreate(loc_id, map_name, H5T_NATIVE_INT, H5T_NATIVE_INT, H5P_DEFAULT,
                                           H5P_DEFAULT, H5P_DEFAULT)) < 0) {
                    H5_FAILED();
                    AT();
                    printf("failed to create map object '%s'\n", map_name);
                    goto error;
                }
            }
        }
        else if (!strcmp(hand.map_dtype, "vl")) {
            if (hand.isAsync) {
                if ((map_id[i] = H5Mcreate_async(loc_id, map_name, map_vl_key_dtype_id, map_vl_value_dtype_id,
                                                 H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT, estack)) < 0) {
                    H5_FAILED();
                    AT();
                    printf("failed to create map object '%s'\n", map_name);
                    goto error;
                }

                if (hand.useWait) {
                    if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed ||
                        num_in_progress) {
                        H5_FAILED();
                        AT();
                        printf("failed to wait for map creation\n");
                        goto error;
                    }
                }
            }
            else {
                if ((map_id[i] = H5Mcreate(loc_id, map_name, map_vl_key_dtype_id, map_vl_value_dtype_id,
                                           H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) < 0) {
                    H5_FAILED();
                    AT();
                    printf("failed to create map object '%s'\n", map_name);
                    goto error;
                }
            }
        }
    }

    if (hand.isAsync) {
        if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed || num_in_progress) {
            H5_FAILED();
            AT();
            printf("failed to wait for map create\n");
            goto error;
        }
    }

    end  = MPI_Wtime();
    time = end - start;
    op_time[MAP_CREATE_NUM][tree_order] += time;

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Map put */
    start = MPI_Wtime();
    for (i = 0; i < hand.numbOfObjs; i++) {
        if (!hand.noMapEntry) {
            if (!strcmp(hand.map_dtype, "int")) {
                for (j = 0; j < hand.numbOfMapEntries; j++)
                    if (hand.isAsync && H5Mput_async(map_id[i], H5T_NATIVE_INT, &map_keys[j], H5T_NATIVE_INT,
                                                     &map_vals[j], H5P_DEFAULT, estack) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to set key-value pair\n");
                        goto error;
                    }
                    else if (!hand.isAsync && H5Mput(map_id[i], H5T_NATIVE_INT, &map_keys[j], H5T_NATIVE_INT,
                                                     &map_vals[j], H5P_DEFAULT) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to set key-value pair\n");
                        goto error;
                    } /* end if */
            }
            else if (!strcmp(hand.map_dtype, "vl")) {
                for (j = 0; j < hand.numbOfMapEntries; j++)
                    if (hand.isAsync &&
                        H5Mput_async(map_id[i], map_vl_key_dtype_id, &vls_vl_keys[j], map_vl_value_dtype_id,
                                     &vls_vl_vals[j], H5P_DEFAULT, estack) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to set key-value pair\n");
                        goto error;
                    }
                    else if (!hand.isAsync &&
                             H5Mput(map_id[i], map_vl_key_dtype_id, &vls_vl_keys[j], map_vl_value_dtype_id,
                                    &vls_vl_vals[j], H5P_DEFAULT) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to set key-value pair\n");
                        goto error;
                    } /* end if */
            }
        }
    }

    if (hand.isAsync) {
        if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed || num_in_progress) {
            H5_FAILED();
            AT();
            printf("failed to wait for map put\n");
            goto error;
        }
    }

    end  = MPI_Wtime();
    time = end - start;
    op_time[MAP_PUT_NUM][tree_order] += time;

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Map close */
    start = MPI_Wtime();
    for (i = 0; i < hand.numbOfObjs; i++) {
        if (hand.isAsync) {
            if (H5Mclose_async(map_id[i], estack) < 0) {
                H5_FAILED();
                AT();
                printf("failed to close map object\n");
                goto error;
            }
        }
        else {
            if (H5Mclose(map_id[i]) < 0) {
                H5_FAILED();
                AT();
                printf("failed to close map object\n");
                goto error;
            }
        }
    }

    if (hand.isAsync) {
        if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed || num_in_progress) {
            H5_FAILED();
            AT();
            printf("failed to wait for map close\n");
            goto error;
        }
    }

    end  = MPI_Wtime();
    time = end - start;
    op_time[MAP_CLOSE_NUM][tree_order] += time;

    if (hand.isAsync && H5Oflush_async(loc_id, estack) < 0) {
        H5_FAILED();
        AT();
        printf("failed to flush dataset object\n");
        goto error;
    }

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Map open */
    start = MPI_Wtime();
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(map_name, "map_object_%d", i + 1);
        if (hand.isAsync) {
            if ((map_id[i] = H5Mopen_async(loc_id, map_name, H5P_DEFAULT, estack)) < 0) {
                H5_FAILED();
                AT();
                printf("failed to open map object\n");
                goto error;
            }

            if (hand.useWait && (i % hand.waitInterval == 0)) {
                if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed ||
                    num_in_progress) {
                    H5_FAILED();
                    AT();
                    printf("failed to wait for map creation\n");
                    goto error;
                }
            }
        }
        else {
            if ((map_id[i] = H5Mopen(loc_id, map_name, H5P_DEFAULT)) < 0) {
                H5_FAILED();
                AT();
                printf("failed to open map object\n");
                goto error;
            }
        }
    }

    if (hand.isAsync) {
        if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed || num_in_progress) {
            H5_FAILED();
            AT();
            printf("failed to wait for map open\n");
            goto error;
        }
    }

    end  = MPI_Wtime();
    time = end - start;
    op_time[MAP_OPEN_NUM][tree_order] += time;

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Map get */
    start = MPI_Wtime();
    for (i = 0; i < hand.numbOfObjs; i++) {
        if (!hand.noMapEntry) {
            if (!strcmp(hand.map_dtype, "int")) {
                for (j = 0; j < hand.numbOfMapEntries; j++)
                    if (hand.isAsync && H5Mget_async(map_id[i], H5T_NATIVE_INT, &map_keys[j], H5T_NATIVE_INT,
                                                     &map_vals_out[j], H5P_DEFAULT, estack) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to get key-value pair\n");
                        goto error;
                    }
                    else if (!hand.isAsync && H5Mget(map_id[i], H5T_NATIVE_INT, &map_keys[j], H5T_NATIVE_INT,
                                                     &map_vals_out[j], H5P_DEFAULT) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to get key-value pair\n");
                        goto error;
                    } /* end if */
            }
            else if (!strcmp(hand.map_dtype, "vl")) {
                for (j = 0; j < hand.numbOfMapEntries; j++)
                    if (hand.isAsync &&
                        H5Mget_async(map_id[i], map_vl_key_dtype_id, &vls_vl_keys[j], map_vl_value_dtype_id,
                                     &vls_vl_out[j], H5P_DEFAULT, estack) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to set key-value pair\n");
                        goto error;
                    }
                    else if (!hand.isAsync &&
                             H5Mget(map_id[i], map_vl_key_dtype_id, &vls_vl_keys[j], map_vl_value_dtype_id,
                                    &vls_vl_out[j], H5P_DEFAULT) < 0) {
                        H5_FAILED();
                        AT();
                        printf("failed to set key-value pair\n");
                        goto error;
                    } /* end if */
            }
        }
    }

    if (hand.isAsync) {
        if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed || num_in_progress) {
            H5_FAILED();
            AT();
            printf("failed to wait for map get\n");
            goto error;
        }
    }

    end  = MPI_Wtime();
    time = end - start;
    op_time[MAP_GET_NUM][tree_order] += time;

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Map close */
    start = MPI_Wtime();
    for (i = 0; i < hand.numbOfObjs; i++) {
        if (hand.isAsync) {
            if (H5Mclose_async(map_id[i], estack) < 0) {
                H5_FAILED();
                AT();
                printf("failed to close map object\n");
                goto error;
            }
        }
        else {
            if (H5Mclose(map_id[i]) < 0) {
                H5_FAILED();
                AT();
                printf("failed to close map object\n");
                goto error;
            }
        }
    }

    if (hand.isAsync) {
        if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed || num_in_progress) {
            H5_FAILED();
            AT();
            printf("failed to wait for map get\n");
            goto error;
        }
    }

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    /* Map Removal */
    start = MPI_Wtime();
    for (i = 0; i < hand.numbOfObjs; i++) {
        sprintf(map_name, "map_object_%d", i + 1);

        if (hand.isAsync && H5Ldelete_async(loc_id, map_name, H5P_DEFAULT, estack) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close map object\n");
            goto error;
        }
        else if (!hand.isAsync && H5Ldelete(loc_id, map_name, H5P_DEFAULT) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close map object\n");
            goto error;
        }
    }

    if (hand.isAsync) {
        if (H5ESwait(estack, UINT64_MAX, &num_in_progress, &op_failed) < 0 || op_failed || num_in_progress) {
            H5_FAILED();
            AT();
            printf("failed to wait for map removal\n");
            goto error;
        }
    }

    end  = MPI_Wtime();
    time = end - start;
    op_time[MAP_REMOVE_NUM][tree_order] += time;

    if (MPI_SUCCESS != MPI_Barrier(MPI_COMM_WORLD)) {
        H5_FAILED();
        AT();
        printf("MPI_Barrier failed\n");
        goto error;
    }

    if (map_id)
        free(map_id);

    if (hand.isAsync && H5ESclose(estack) < 0) {
        H5_FAILED();
        AT();
        printf("failed to close event set\n");
        goto error;
    }

    return 0;

error:
    if (map_id)
        free(map_id);

    if (hand.isAsync && H5ESclose(estack) < 0) {
        H5_FAILED();
        AT();
        printf("failed to close event set\n");
        goto error;
    }

    return -1;
}

static int
create_objects_in_tree_node(hid_t tree_node_gid)
{
    hid_t loc_id = H5I_INVALID_HID;
    char  unique_group_per_rank_name[NAME_LENGTH];

    /* Create a unique group for each rank (-u command-line option) */
    if (hand.uniqueGroupPerRank) {
        sprintf(unique_group_per_rank_name, "unique_group_per_rank_%d", mpi_rank);

        /* Use SX object class through GCPL_ID to create this group */
        if ((loc_id = H5Gcreate2(tree_node_gid, unique_group_per_rank_name, H5P_DEFAULT, gcpl_id,
                                 H5P_DEFAULT)) < 0) {
            H5_FAILED();
            AT();
            printf("    couldn't create unique group in the tree node '%s'\n", unique_group_per_rank_name);
            goto error;
        }
    }
    else
        loc_id = tree_node_gid;

    /* Test group object. Test for link is also included */
    if ((hand.testAllObjects || hand.testGroupOnly) && test_group(loc_id) < 0) {
        H5_FAILED();
        AT();
        printf("    couldn't test group objects in the tree node\n");
        goto error;
    }

    /* Test dataset object. */
    if ((hand.testAllObjects || hand.testDsetOnly) && test_dataset(loc_id) < 0) {
        H5_FAILED();
        AT();
        printf("    couldn't test dataset objects in the tree node\n");
        goto error;
    }

    /* Test attribute object. */
    if ((hand.testAllObjects || hand.testAttrOnly) && test_attribute(loc_id) < 0) {
        H5_FAILED();
        AT();
        printf("    couldn't test attribute objects in the tree node\n");
        goto error;
    }

    /* Test datatype object. */
    if ((hand.testAllObjects || hand.testDtypeOnly) && test_datatype(loc_id) < 0) {
        H5_FAILED();
        AT();
        printf("    couldn't test datatype objects in the tree node\n");
        goto error;
    }

    /* Test H5O API. */
    if ((hand.testAllObjects || hand.testObjectOnly) && test_H5O(loc_id) < 0) {
        H5_FAILED();
        AT();
        printf("    couldn't test H5O API in the tree node\n");
        goto error;
    }

    /* Test map object.  Map object only works for H5VOL */
    if (!hand.runMPIIO && (hand.testAllObjects || hand.testMapOnly) && test_map(loc_id) < 0) {
        H5_FAILED();
        AT();
        printf("    couldn't test map objects in the tree node\n");
        goto error;
    }

    /* Close the unique group per rank */
    if (hand.uniqueGroupPerRank) {
        if (H5Gclose(loc_id) < 0) {
            H5_FAILED();
            AT();
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
    hid_t  loc_file_id;
    char   filename[NAME_LENGTH];
    double start, end, time;
    int    i;

    sprintf(filename, "%s", hand.fileName);

    for (i = 0; i < hand.numbOfFiles; i++) {
        /* File creation */
        start = MPI_Wtime();

        if ((loc_file_id = H5Fcreate(filename, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0) {
            H5_FAILED();
            AT();
            printf("failed to create the file '%s'\n", filename);
            goto error;
        }

        end                              = MPI_Wtime();
        time                             = end - start;
        file_op_time[FILE_CREATE_NUM][i] = time;

#ifdef DEBUG
        printf("File creation time: %lf, mpi_rank=%d\n", time, mpi_rank);
#endif

        start = MPI_Wtime();

        if (H5Fclose(loc_file_id) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close the file\n");
            goto error;
        }

        end                             = MPI_Wtime();
        time                            = end - start;
        file_op_time[FILE_CLOSE_NUM][i] = time;

#ifdef DEBUG
        printf("File close time: %lf, mpi_rank=%d\n", time, mpi_rank);
#endif

        /* File open */
        start = MPI_Wtime();

        if ((loc_file_id = H5Fopen(filename, H5F_ACC_RDWR, fapl_id)) < 0) {
            H5_FAILED();
            AT();
            printf("failed to open the file '%s'\n", filename);
            goto error;
        }

        end                            = MPI_Wtime();
        time                           = end - start;
        file_op_time[FILE_OPEN_NUM][i] = time;

#ifdef DEBUG
        printf("File open time: %lf, mpi_rank=%d\n", time, mpi_rank);
#endif

        if (H5Fclose(loc_file_id) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close the file\n");
            goto error;
        }

#ifdef TMP
        /* File delete - Temporariely disabled until the hanging problem is fixed (Jira issue ID-245) */
        start = MPI_Wtime();

        /* H5Fdelete only works for H5VOL */
        if (!hand.runMPIIO) {
            if (H5Fdelete(filename, fapl_id) < 0) {
                H5_FAILED();
                AT();
                printf("failed to delete the file '%s'\n", filename);
                goto error;
            }
        }
        else {
            if (MPI_File_delete(filename, MPI_INFO_NULL) < 0) {
                H5_FAILED();
                AT();
                printf("failed to delete the file '%s'\n", filename);
                goto error;
            }
        }

        end                              = MPI_Wtime();
        time                             = end - start;
        file_op_time[FILE_REMOVE_NUM][i] = time;

#ifdef DEBUG
        printf("File removal time: %lf, mpi_rank=%d\n", time, mpi_rank);
#endif
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

    for (i = 0; i < hand.numbOfBranches; i++) {
        sprintf(gname, "child_group_depth_%d_branch_%d", depth, i + 1);

        if ((child_gid = H5Gcreate2(parent_gid, gname, H5P_DEFAULT, H5P_DEFAULT, gapl_id)) < 0) {
            H5_FAILED();
            AT();
            printf("    couldn't create group '%s'\n", gname);
            goto error;
        }

        if (create_objects_in_tree_node(child_gid) < 0) {
            H5_FAILED();
            AT();
            printf("    couldn't create objects in the tree root node\n");
            goto error;
        }

        if (depth < (unsigned)hand.depthOfTree) {
            if (create_tree_preorder(child_gid, depth + 1) < 0) {
                H5_FAILED();
                AT();
                printf("failed to do preorder tree traversal \n");
                goto error;
            }
        }

        if (H5Gclose(child_gid) < 0) {
            H5_FAILED();
            AT();
            printf("failed to close the group '%s'\n", gname);
            goto error;
        }
    }

    return 0;

error:
    H5E_BEGIN_TRY
    {
        H5Gclose(child_gid);
    }
    H5E_END_TRY;

    return -1;
}

static int
create_trees(hid_t file)
{
    hid_t tree_root_id;
    char  tree_root_name[NAME_LENGTH];
    int   i;

    for (i = 0; i < hand.numbOfTrees; i++) {
        snprintf(tree_root_name, NAME_LENGTH, "tree_root_order_%d", i);

        tree_order = (unsigned)i;

        /* Create the group as the tree root */
        if ((tree_root_id = H5Gcreate2(file, tree_root_name, H5P_DEFAULT, H5P_DEFAULT, gapl_id)) < 0) {
            H5_FAILED();
            AT();
            printf("    couldn't create group as tree root '%s'\n", tree_root_name);
            goto error;
        }

        if (create_objects_in_tree_node(tree_root_id) < 0) {
            H5_FAILED();
            AT();
            printf("    couldn't create objects in the tree root node\n");
            goto error;
        }

        if (0 < hand.depthOfTree) {
            if (create_tree_preorder(tree_root_id, 1) < 0) {
                H5_FAILED();
                AT();
                printf("    couldn't create tree branches\n");
                goto error;
            }
        }

        if (H5Gclose(tree_root_id) < 0) {
            H5_FAILED();
            AT();
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
main(int argc, char **argv)
{
    hid_t fapl_id = -1 /*, file_id = -1*/;
    int   nerrors = 0;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);

    if (parse_command_line(argc, argv) < 0) {
        nerrors++;
        goto error;
    }

    initialize_time();

    initialize_data();

    if ((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0) {
        nerrors++;
        goto error;
    }

    if (H5Pset_fapl_mpio(fapl_id, MPI_COMM_WORLD, MPI_INFO_NULL) < 0) {
        nerrors++;
        goto error;
    }

    if (hand.uniqueGroupPerRank) {
        if (H5daos_set_all_ind_metadata_ops(fapl_id, TRUE) < 0) {
            nerrors++;
            goto error;
        }
    }
    else {
        if (H5Pset_all_coll_metadata_ops(fapl_id, TRUE) < 0) {
            nerrors++;
            goto error;
        }
    }

    if (create_ids() < 0) {
        nerrors++;
        goto error;
    }

    /* If we are testing anything except file operations, go into this block */
    if (!hand.testFileOnly) {
        if ((file_id = H5Fcreate(hand.fileName, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) < 0) {
            nerrors++;
            goto error;
        }

        /* Create trees */
        if (create_trees(file_id) < 0) {
            nerrors++;
            goto error;
        }

        if (H5Fclose(file_id) < 0) {
            nerrors++;
            goto error;
        }
    }

    if (hand.testAllObjects || hand.testFileOnly) {
        /* For file operations, make sure it's collecitve */
        if (hand.uniqueGroupPerRank) {
            if (H5daos_set_all_ind_metadata_ops(fapl_id, FALSE) < 0) {
                nerrors++;
                goto error;
            }
        }

        /* File operations */
        if (operate_on_files(fapl_id) < 0) {
            nerrors++;
            goto error;
        }
    }

    if (H5Pclose(fapl_id) < 0) {
        nerrors++;
        goto error;
    }

    calculate_results();

    if (release_resources() < 0) {
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
