/*
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
 * Purpose: Contains macros to facilitate testing the DAOS VOL plugin.
 */

#ifndef TEST_DAOS_VOL_H
#define TEST_DAOS_VOL_H

/* The names of a set of container groups which hold objects
 * created by each of the different types of tests
 */
#define GROUP_TEST_GROUP_NAME         "group_tests"
#define ATTRIBUTE_TEST_GROUP_NAME     "attribute_tests"
#define DATASET_TEST_GROUP_NAME       "dataset_tests"
#define DATATYPE_TEST_GROUP_NAME      "datatype_tests"
#define LINK_TEST_GROUP_NAME          "link_tests"
#define OBJECT_TEST_GROUP_NAME        "object_tests"
#define MISCELLANEOUS_TEST_GROUP_NAME "miscellaneous_tests"


/*****************************************************
 *                                                   *
 *             Plugin File test defines              *
 *                                                   *
 *****************************************************/

#define FILE_INTENT_TEST_DATASETNAME "/test_dset"
#define FILE_INTENT_TEST_DSET_RANK   2
#define FILE_INTENT_TEST_FILENAME    "intent_test_file"

#define NONEXISTENT_FILENAME "nonexistent_file"

#define FILE_PROPERTY_LIST_TEST_FNAME1 "property_list_test_file1"
#define FILE_PROPERTY_LIST_TEST_FNAME2 "property_list_test_file2"


/*****************************************************
 *                                                   *
 *             Plugin Group test defines             *
 *                                                   *
 *****************************************************/

#define GROUP_CREATE_INVALID_LOC_ID_GNAME    "/test_group"

#define GROUP_CREATE_UNDER_ROOT_GNAME        "/group_under_root"

#define GROUP_CREATE_UNDER_GROUP_REL_GNAME   "group_under_group2"

#define GROUP_CREATE_ANONYMOUS_GROUP_NAME    "anon_group"

#define NONEXISTENT_GROUP_TEST_GNAME         "/nonexistent_group"

#define GROUP_PROPERTY_LIST_TEST_GROUP_NAME1 "property_list_test_group1"
#define GROUP_PROPERTY_LIST_TEST_GROUP_NAME2 "property_list_test_group2"
#define GROUP_PROPERTY_LIST_TEST_DUMMY_VAL   100


/*****************************************************
 *                                                   *
 *           Plugin Attribute test defines           *
 *                                                   *
 *****************************************************/

#define ATTRIBUTE_CREATE_ON_ROOT_SPACE_RANK 2
#define ATTRIBUTE_CREATE_ON_ROOT_ATTR_NAME  "attr_on_root"
#define ATTRIBUTE_CREATE_ON_ROOT_ATTR_NAME2 "attr_on_root2"

#define ATTRIBUTE_CREATE_ON_DATASET_DSET_SPACE_RANK 2
#define ATTRIBUTE_CREATE_ON_DATASET_ATTR_SPACE_RANK 2
#define ATTRIBUTE_CREATE_ON_DATASET_DSET_NAME       "dataset_with_attr"
#define ATTRIBUTE_CREATE_ON_DATASET_ATTR_NAME       "attr_on_dataset"
#define ATTRIBUTE_CREATE_ON_DATASET_ATTR_NAME2      "attr_on_dataset2"

#define ATTRIBUTE_CREATE_ON_DATATYPE_SPACE_RANK 2
#define ATTRIBUTE_CREATE_ON_DATATYPE_DTYPE_NAME "datatype_with_attr"
#define ATTRIBUTE_CREATE_ON_DATATYPE_ATTR_NAME  "attr_on_datatype"
#define ATTRIBUTE_CREATE_ON_DATATYPE_ATTR_NAME2 "attr_on_datatype2"

#define ATTRIBUTE_CREATE_NULL_DATASPACE_TEST_SUBGROUP_NAME "attr_with_null_space_test"
#define ATTRIBUTE_CREATE_NULL_DATASPACE_TEST_ATTR_NAME     "attr_with_null_space"

#define ATTRIBUTE_CREATE_SCALAR_DATASPACE_TEST_SUBGROUP_NAME "attr_with_scalar_space_test"
#define ATTRIBUTE_CREATE_SCALAR_DATASPACE_TEST_ATTR_NAME     "attr_with_scalar_space"

#define ATTRIBUTE_GET_INFO_TEST_SPACE_RANK 2
#define ATTRIBUTE_GET_INFO_TEST_ATTR_NAME  "get_info_test_attr"

#define ATTRIBUTE_GET_SPACE_TYPE_TEST_SPACE_RANK 2
#define ATTRIBUTE_GET_SPACE_TYPE_TEST_ATTR_NAME  "get_space_type_test_attr"

#define ATTRIBUTE_GET_NAME_TEST_ATTRIBUTE_NAME "retrieve_attr_name_test"
#define ATTRIBUTE_GET_NAME_TEST_SPACE_RANK     2

#define ATTRIBUTE_CREATE_WITH_SPACE_IN_NAME_SPACE_RANK 2
#define ATTRIBUTE_CREATE_WITH_SPACE_IN_NAME_ATTR_NAME  "attr with space in name"

#define ATTRIBUTE_DELETION_TEST_SPACE_RANK 2
#define ATTRIBUTE_DELETION_TEST_ATTR_NAME  "attr_to_be_deleted"

#define ATTRIBUTE_WRITE_TEST_ATTR_DTYPE_SIZE sizeof(int)
#define ATTRIBUTE_WRITE_TEST_ATTR_DTYPE      H5T_NATIVE_INT
#define ATTRIBUTE_WRITE_TEST_SPACE_RANK      2
#define ATTRIBUTE_WRITE_TEST_ATTR_NAME       "write_test_attr"

#define ATTRIBUTE_READ_TEST_ATTR_DTYPE_SIZE sizeof(int)
#define ATTRIBUTE_READ_TEST_ATTR_DTYPE      H5T_NATIVE_INT
#define ATTRIBUTE_READ_TEST_SPACE_RANK      2
#define ATTRIBUTE_READ_TEST_ATTR_NAME       "read_test_attr"

#define ATTRIBUTE_RENAME_TEST_SPACE_RANK 2
#define ATTRIBUTE_RENAME_TEST_ATTR_NAME  "rename_test_attr"
#define ATTRIBUTE_RENAME_TEST_NEW_NAME   "renamed_attr"

#define ATTRIBUTE_GET_NUM_ATTRS_TEST_ATTRIBUTE_NAME "get_num_attrs_test_attribute"
#define ATTRIBUTE_GET_NUM_ATTRS_TEST_SPACE_RANK     2

#define ATTRIBUTE_ITERATE_TEST_DSET_SPACE_RANK 2
#define ATTRIBUTE_ITERATE_TEST_ATTR_SPACE_RANK 2
#define ATTRIBUTE_ITERATE_TEST_SUBGROUP_NAME   "attribute_iterate_test"
#define ATTRIBUTE_ITERATE_TEST_DSET_NAME       "attribute_iterate_dset"
#define ATTRIBUTE_ITERATE_TEST_ATTR_NAME       "iter_attr1"
#define ATTRIBUTE_ITERATE_TEST_ATTR_NAME2      "iter_attr2"
#define ATTRIBUTE_ITERATE_TEST_ATTR_NAME3      "iter_attr3"
#define ATTRIBUTE_ITERATE_TEST_ATTR_NAME4      "iter_attr4"

#define ATTRIBUTE_ITERATE_TEST_0_ATTRIBUTES_DSET_SPACE_RANK 2
#define ATTRIBUTE_ITERATE_TEST_0_ATTRIBUTES_SUBGROUP_NAME   "attribute_iterate_test_0_attributes"
#define ATTRIBUTE_ITERATE_TEST_0_ATTRIBUTES_DSET_NAME       "attribute_iterate_dset"

#define ATTRIBUTE_UNUSED_APIS_TEST_SPACE_RANK 2
#define ATTRIBUTE_UNUSED_APIS_TEST_ATTR_NAME  "unused_apis_attr"

#define ATTRIBUTE_PROPERTY_LIST_TEST_ATTRIBUTE_NAME1 "property_list_test_attribute1"
#define ATTRIBUTE_PROPERTY_LIST_TEST_ATTRIBUTE_NAME2 "property_list_test_attribute2"
#define ATTRIBUTE_PROPERTY_LIST_TEST_SUBGROUP_NAME   "attribute_property_list_test_group"
#define ATTRIBUTE_PROPERTY_LIST_TEST_SPACE_RANK      2


/*****************************************************
 *                                                   *
 *            Plugin Dataset test defines            *
 *                                                   *
 *****************************************************/

#define DATASET_CREATE_UNDER_ROOT_DSET_NAME  "/dset_under_root"
#define DATASET_CREATE_UNDER_ROOT_SPACE_RANK 2

#define DATASET_CREATE_ANONYMOUS_DATASET_NAME "anon_dset"
#define DATASET_CREATE_ANONYMOUS_SPACE_RANK   2

#define DATASET_CREATE_UNDER_EXISTING_SPACE_RANK 2
#define DATASET_CREATE_UNDER_EXISTING_DSET_NAME  "nested_dset"

#define DATASET_CREATE_NULL_DATASPACE_TEST_SUBGROUP_NAME "dataset_with_null_space_test"
#define DATASET_CREATE_NULL_DATASPACE_TEST_DSET_NAME     "dataset_with_null_space"

#define DATASET_CREATE_SCALAR_DATASPACE_TEST_SUBGROUP_NAME "dataset_with_scalar_space_test"
#define DATASET_CREATE_SCALAR_DATASPACE_TEST_DSET_NAME     "dataset_with_scalar_space"

/* Defines for testing the plugin's ability to parse different types
 * of Datatypes for Dataset creation
 */
#define DATASET_PREDEFINED_TYPE_TEST_SPACE_RANK    2
#define DATASET_PREDEFINED_TYPE_TEST_BASE_NAME     "predefined_type_dset"
#define DATASET_PREDEFINED_TYPE_TEST_SUBGROUP_NAME "predefined_type_dataset_test"

#define DATASET_STRING_TYPE_TEST_STRING_LENGTH  40
#define DATASET_STRING_TYPE_TEST_SPACE_RANK     2
#define DATASET_STRING_TYPE_TEST_DSET_NAME1     "fixed_length_string_dset"
#define DATASET_STRING_TYPE_TEST_DSET_NAME2     "variable_length_string_dset"
#define DATASET_STRING_TYPE_TEST_SUBGROUP_NAME  "string_type_dataset_test"

#define DATASET_ENUM_TYPE_TEST_VAL_BASE_NAME "INDEX"
#define DATASET_ENUM_TYPE_TEST_SUBGROUP_NAME "enum_type_dataset_test"
#define DATASET_ENUM_TYPE_TEST_NUM_MEMBERS   16
#define DATASET_ENUM_TYPE_TEST_SPACE_RANK    2
#define DATASET_ENUM_TYPE_TEST_DSET_NAME1    "enum_native_dset"
#define DATASET_ENUM_TYPE_TEST_DSET_NAME2    "enum_non_native_dset"

#define DATASET_ARRAY_TYPE_TEST_SUBGROUP_NAME "array_type_dataset_test"
#define DATASET_ARRAY_TYPE_TEST_DSET_NAME1    "array_type_test1"
#define DATASET_ARRAY_TYPE_TEST_DSET_NAME2    "array_type_test2"
#define DATASET_ARRAY_TYPE_TEST_DSET_NAME3    "array_type_test3"
#define DATASET_ARRAY_TYPE_TEST_SPACE_RANK    2
#define DATASET_ARRAY_TYPE_TEST_RANK1         2
#define DATASET_ARRAY_TYPE_TEST_RANK2         2
#define DATASET_ARRAY_TYPE_TEST_RANK3         2

#define DATASET_COMPOUND_TYPE_TEST_SUBGROUP_NAME "compound_type_dataset_test"
#define DATASET_COMPOUND_TYPE_TEST_DSET_NAME     "compound_type_test"
#define DATASET_COMPOUND_TYPE_TEST_MAX_SUBTYPES  10
#define DATASET_COMPOUND_TYPE_TEST_MAX_PASSES    5
#define DATASET_COMPOUND_TYPE_TEST_DSET_RANK     2

/* Defines for testing the plugin's ability to parse different
 * Dataset shapes for creation
 */
#define DATASET_SHAPE_TEST_DSET_BASE_NAME "dataset_shape_test"
#define DATASET_SHAPE_TEST_SUBGROUP_NAME  "dataset_shape_test"
#define DATASET_SHAPE_TEST_NUM_ITERATIONS 5
#define DATASET_SHAPE_TEST_MAX_DIMS       32

#define DATASET_CREATION_PROPERTIES_TEST_TRACK_TIMES_YES_DSET_NAME "track_times_true_test"
#define DATASET_CREATION_PROPERTIES_TEST_TRACK_TIMES_NO_DSET_NAME  "track_times_false_test"
#define DATASET_CREATION_PROPERTIES_TEST_PHASE_CHANGE_DSET_NAME    "attr_phase_change_test"
#define DATASET_CREATION_PROPERTIES_TEST_ALLOC_TIMES_BASE_NAME     "alloc_time_test"
#define DATASET_CREATION_PROPERTIES_TEST_FILL_TIMES_BASE_NAME      "fill_times_test"
#define DATASET_CREATION_PROPERTIES_TEST_CRT_ORDER_BASE_NAME       "creation_order_test"
#define DATASET_CREATION_PROPERTIES_TEST_LAYOUTS_BASE_NAME         "layout_test"
#define DATASET_CREATION_PROPERTIES_TEST_FILTERS_DSET_NAME         "filters_test"
#define DATASET_CREATION_PROPERTIES_TEST_GROUP_NAME                "creation_properties_test"
#define DATASET_CREATION_PROPERTIES_TEST_CHUNK_DIM_RANK            DATASET_CREATION_PROPERTIES_TEST_SHAPE_RANK
#define DATASET_CREATION_PROPERTIES_TEST_MAX_COMPACT               12
#define DATASET_CREATION_PROPERTIES_TEST_MIN_DENSE                 8
#define DATASET_CREATION_PROPERTIES_TEST_SHAPE_RANK                3

#define DATASET_SMALL_WRITE_TEST_ALL_DSET_SPACE_RANK 3
#define DATASET_SMALL_WRITE_TEST_ALL_DSET_DTYPESIZE  sizeof(int)
#define DATASET_SMALL_WRITE_TEST_ALL_DSET_DTYPE      H5T_NATIVE_INT
#define DATASET_SMALL_WRITE_TEST_ALL_DSET_NAME       "dataset_write_small_all"

#define DATASET_SMALL_WRITE_TEST_HYPERSLAB_DSET_SPACE_RANK 3
#define DATASET_SMALL_WRITE_TEST_HYPERSLAB_DSET_DTYPESIZE  sizeof(int)
#define DATASET_SMALL_WRITE_TEST_HYPERSLAB_DSET_DTYPE      H5T_NATIVE_INT
#define DATASET_SMALL_WRITE_TEST_HYPERSLAB_DSET_NAME       "dataset_write_small_hyperslab"

#define DATASET_SMALL_WRITE_TEST_POINT_SELECTION_DSET_SPACE_RANK 3
#define DATASET_SMALL_WRITE_TEST_POINT_SELECTION_DSET_DTYPESIZE  sizeof(int)
#define DATASET_SMALL_WRITE_TEST_POINT_SELECTION_DSET_DTYPE      H5T_NATIVE_INT
#define DATASET_SMALL_WRITE_TEST_POINT_SELECTION_NUM_POINTS      10
#define DATASET_SMALL_WRITE_TEST_POINT_SELECTION_DSET_NAME       "dataset_write_small_point_selection"

#ifndef NO_LARGE_TESTS
#define DATASET_LARGE_WRITE_TEST_ALL_DSET_SPACE_RANK 3
#define DATASET_LARGE_WRITE_TEST_ALL_DSET_DTYPESIZE  sizeof(int)
#define DATASET_LARGE_WRITE_TEST_ALL_DSET_DTYPE      H5T_NATIVE_INT
#define DATASET_LARGE_WRITE_TEST_ALL_DSET_NAME       "dataset_write_large_all"

#define DATASET_LARGE_WRITE_TEST_HYPERSLAB_DSET_SPACE_RANK 3
#define DATASET_LARGE_WRITE_TEST_HYPERSLAB_DSET_DTYPESIZE  sizeof(int)
#define DATASET_LARGE_WRITE_TEST_HYPERSLAB_DSET_DTYPE      H5T_NATIVE_INT
#define DATASET_LARGE_WRITE_TEST_HYPERSLAB_DSET_NAME       "dataset_write_large_hyperslab"

#define DATASET_LARGE_WRITE_TEST_POINT_SELECTION_DSET_SPACE_RANK 3
#define DATASET_LARGE_WRITE_TEST_POINT_SELECTION_DSET_DTYPESIZE  sizeof(int)
#define DATASET_LARGE_WRITE_TEST_POINT_SELECTION_DSET_DTYPE      H5T_NATIVE_INT
#define DATASET_LARGE_WRITE_TEST_POINT_SELECTION_DSET_NAME       "dataset_write_large_point_selection"
#endif

#define DATASET_SMALL_READ_TEST_ALL_DSET_SPACE_RANK 3
#define DATASET_SMALL_READ_TEST_ALL_DSET_DTYPESIZE  sizeof(int)
#define DATASET_SMALL_READ_TEST_ALL_DSET_DTYPE      H5T_NATIVE_INT
#define DATASET_SMALL_READ_TEST_ALL_DSET_NAME       "dataset_read_small_all"

#define DATASET_SMALL_READ_TEST_HYPERSLAB_DSET_SPACE_RANK 3
#define DATASET_SMALL_READ_TEST_HYPERSLAB_DSET_DTYPESIZE  sizeof(int)
#define DATASET_SMALL_READ_TEST_HYPERSLAB_DSET_DTYPE      H5T_NATIVE_INT
#define DATASET_SMALL_READ_TEST_HYPERSLAB_DSET_NAME       "dataset_read_small_hyperslab"

#define DATASET_SMALL_READ_TEST_POINT_SELECTION_DSET_SPACE_RANK 3
#define DATASET_SMALL_READ_TEST_POINT_SELECTION_DSET_DTYPESIZE  sizeof(int)
#define DATASET_SMALL_READ_TEST_POINT_SELECTION_DSET_DTYPE      H5T_NATIVE_INT
#define DATASET_SMALL_READ_TEST_POINT_SELECTION_NUM_POINTS      10
#define DATASET_SMALL_READ_TEST_POINT_SELECTION_DSET_NAME       "dataset_read_small_point_selection"

#ifndef NO_LARGE_TESTS
#define DATASET_LARGE_READ_TEST_ALL_DSET_SPACE_RANK 3
#define DATASET_LARGE_READ_TEST_ALL_DSET_DTYPESIZE  sizeof(int)
#define DATASET_LARGE_READ_TEST_ALL_DSET_DTYPE      H5T_NATIVE_INT
#define DATASET_LARGE_READ_TEST_ALL_DSET_NAME       "dataset_read_large_all"

#define DATASET_LARGE_READ_TEST_HYPERSLAB_DSET_SPACE_RANK 3
#define DATASET_LARGE_READ_TEST_HYPERSLAB_DSET_DTYPESIZE  sizeof(int)
#define DATASET_LARGE_READ_TEST_HYPERSLAB_DSET_DTYPE      H5T_NATIVE_INT
#define DATASET_LARGE_READ_TEST_HYPERSLAB_DSET_NAME       "dataset_read_large_hyperslab"

#define DATASET_LARGE_READ_TEST_POINT_SELECTION_DSET_SPACE_RANK 3
#define DATASET_LARGE_READ_TEST_POINT_SELECTION_DSET_DTYPESIZE  sizeof(int)
#define DATASET_LARGE_READ_TEST_POINT_SELECTION_DSET_DTYPE      H5T_NATIVE_INT
#define DATASET_LARGE_READ_TEST_POINT_SELECTION_DSET_NAME       "dataset_read_large_point_selection"
#endif

#define DATASET_DATA_VERIFY_WRITE_TEST_DSET_SPACE_RANK 3
#define DATASET_DATA_VERIFY_WRITE_TEST_DSET_DTYPESIZE  sizeof(int)
#define DATASET_DATA_VERIFY_WRITE_TEST_DSET_DTYPE      H5T_NATIVE_INT
#define DATASET_DATA_VERIFY_WRITE_TEST_NUM_POINTS      10
#define DATASET_DATA_VERIFY_WRITE_TEST_DSET_NAME       "dataset_data_verification"

#define DATASET_SET_EXTENT_TEST_SPACE_RANK 2
#define DATASET_SET_EXTENT_TEST_DSET_NAME  "set_extent_test_dset"

#define DATASET_UNUSED_APIS_TEST_SPACE_RANK 2
#define DATASET_UNUSED_APIS_TEST_DSET_NAME  "unused_apis_dset"


#define DATASET_PROPERTY_LIST_TEST_SUBGROUP_NAME "dataset_property_list_test_group"
#define DATASET_PROPERTY_LIST_TEST_SPACE_RANK    2
#define DATASET_PROPERTY_LIST_TEST_DSET_NAME1    "property_list_test_dataset1"
#define DATASET_PROPERTY_LIST_TEST_DSET_NAME2    "property_list_test_dataset2"
#define DATASET_PROPERTY_LIST_TEST_DSET_NAME3    "property_list_test_dataset3"
#define DATASET_PROPERTY_LIST_TEST_DSET_NAME4    "property_list_test_dataset4"


/*****************************************************
 *                                                   *
 *           Plugin Datatype test defines            *
 *                                                   *
 *****************************************************/

#define DATATYPE_CREATE_TEST_DATASET_DIMS  2

#define DATATYPE_CREATE_TEST_TYPE_NAME "test_type"

#define DATATYPE_CREATE_ANONYMOUS_TYPE_NAME "anon_type"

#define DATASET_CREATE_WITH_DATATYPE_TEST_DATASET_DIMS 2
#define DATASET_CREATE_WITH_DATATYPE_TEST_TYPE_NAME    "committed_type_test_dtype1"
#define DATASET_CREATE_WITH_DATATYPE_TEST_DSET_NAME    "committed_type_test_dset"

#define ATTRIBUTE_CREATE_WITH_DATATYPE_TEST_SPACE_RANK 2
#define ATTRIBUTE_CREATE_WITH_DATATYPE_TEST_DTYPE_NAME "committed_type_test_dtype2"
#define ATTRIBUTE_CREATE_WITH_DATATYPE_TEST_ATTR_NAME  "committed_type_test_attr"

#define DATATYPE_DELETE_TEST_DTYPE_NAME "delete_test_dtype"

#define DATATYPE_PROPERTY_LIST_TEST_SUBGROUP_NAME  "datatype_property_list_test_group"
#define DATATYPE_PROPERTY_LIST_TEST_DATATYPE_NAME1 "property_list_test_datatype1"
#define DATATYPE_PROPERTY_LIST_TEST_DATATYPE_NAME2 "property_list_test_datatype2"


/*****************************************************
 *                                                   *
 *             Plugin Link test defines              *
 *                                                   *
 *****************************************************/

#define HARD_LINK_TEST_LINK_NAME     "test_link"

#define H5L_SAME_LOC_TEST_DSET_SPACE_RANK 2
#define H5L_SAME_LOC_TEST_GROUP_NAME      "h5l_same_loc_test_group"
#if 0
#define H5L_SAME_LOC_TEST_LINK_NAME1      "h5l_same_loc_test_link1"
#endif
#define H5L_SAME_LOC_TEST_LINK_NAME2      "h5l_same_loc_test_link2"
#define H5L_SAME_LOC_TEST_DSET_NAME       "h5l_same_loc_test_dset"

#define SOFT_LINK_EXISTING_RELATIVE_TEST_DSET_SPACE_RANK 2
#define SOFT_LINK_EXISTING_RELATIVE_TEST_SUBGROUP_NAME   "soft_link_to_existing_relative_path_test"
#define SOFT_LINK_EXISTING_RELATIVE_TEST_DSET_NAME       "dset"
#define SOFT_LINK_EXISTING_RELATIVE_TEST_LINK_NAME       "soft_link_to_existing_relative_path"

#define SOFT_LINK_EXISTING_ABSOLUTE_TEST_SUBGROUP_NAME "soft_link_to_existing_absolute_path_test"
#define SOFT_LINK_EXISTING_ABSOLUTE_TEST_LINK_NAME     "soft_link_to_existing_absolute_path"

#define SOFT_LINK_DANGLING_RELATIVE_TEST_DSET_SPACE_RANK 2
#define SOFT_LINK_DANGLING_RELATIVE_TEST_SUBGROUP_NAME   "soft_link_dangling_relative_path_test"
#define SOFT_LINK_DANGLING_RELATIVE_TEST_DSET_NAME       "dset"
#define SOFT_LINK_DANGLING_RELATIVE_TEST_LINK_NAME       "soft_link_dangling_relative_path"

#define SOFT_LINK_DANGLING_ABSOLUTE_TEST_DSET_SPACE_RANK 2
#define SOFT_LINK_DANGLING_ABSOLUTE_TEST_SUBGROUP_NAME   "soft_link_dangling_absolute_path_test"
#define SOFT_LINK_DANGLING_ABSOLUTE_TEST_DSET_NAME       "dset"
#define SOFT_LINK_DANGLING_ABSOLUTE_TEST_LINK_NAME       "soft_link_dangling_absolute_path"

#define EXTERNAL_LINK_TEST_SUBGROUP_NAME "external_link_test"
#define EXTERNAL_LINK_TEST_FILE_NAME     "ext_link_file"
#define EXTERNAL_LINK_TEST_LINK_NAME     "ext_link"

#define EXTERNAL_LINK_TEST_DANGLING_DSET_SPACE_RANK 2
#define EXTERNAL_LINK_TEST_DANGLING_SUBGROUP_NAME   "external_link_dangling_test"
#define EXTERNAL_LINK_TEST_DANGLING_LINK_NAME       "dangling_ext_link"
#define EXTERNAL_LINK_TEST_DANGLING_DSET_NAME       "external_dataset"

#define UD_LINK_TEST_UDATA_MAX_SIZE 256
#define UD_LINK_TEST_LINK_NAME      "ud_link"

#define LINK_DELETE_TEST_DSET_SPACE_RANK     2
#define LINK_DELETE_TEST_EXTERNAL_LINK_NAME  "external_link"
#define LINK_DELETE_TEST_EXTERNAL_LINK_NAME2 "external_link2"
#define LINK_DELETE_TEST_SOFT_LINK_NAME      "soft_link"
#define LINK_DELETE_TEST_SOFT_LINK_NAME2     "soft_link2"
#define LINK_DELETE_TEST_SUBGROUP_NAME       "link_delete_test"
#define LINK_DELETE_TEST_DSET_NAME1          "link_delete_test_dset1"
#define LINK_DELETE_TEST_DSET_NAME2          "link_delete_test_dset2"

#define COPY_LINK_TEST_SOFT_LINK_TARGET_PATH "/" COPY_LINK_TEST_GROUP_NAME "/" COPY_LINK_TEST_DSET_NAME
#define COPY_LINK_TEST_HARD_LINK_COPY_NAME   "hard_link_to_dset_copy"
#define COPY_LINK_TEST_SOFT_LINK_COPY_NAME   "soft_link_to_dset_copy"
#define COPY_LINK_TEST_HARD_LINK_NAME        "hard_link_to_dset"
#define COPY_LINK_TEST_SOFT_LINK_NAME        "soft_link_to_dset"
#define COPY_LINK_TEST_GROUP_NAME            "link_copy_test_group"
#define COPY_LINK_TEST_DSET_NAME             "link_copy_test_dset"
#define COPY_LINK_TEST_DSET_SPACE_RANK       2

#define MOVE_LINK_TEST_SOFT_LINK_TARGET_PATH "/" MOVE_LINK_TEST_GROUP_NAME "/" MOVE_LINK_TEST_DSET_NAME
#define MOVE_LINK_TEST_HARD_LINK_NAME        "hard_link_to_dset"
#define MOVE_LINK_TEST_SOFT_LINK_NAME        "soft_link_to_dset"
#define MOVE_LINK_TEST_GROUP_NAME            "link_move_test_group"
#define MOVE_LINK_TEST_DSET_NAME             "link_move_test_dset"
#define MOVE_LINK_TEST_DSET_SPACE_RANK       2

#define GET_LINK_INFO_TEST_DSET_SPACE_RANK 2
#define GET_LINK_INFO_TEST_SUBGROUP_NAME   "get_link_info_test"
#define GET_LINK_INFO_TEST_SOFT_LINK_NAME  "soft_link"
#define GET_LINK_INFO_TEST_EXT_LINK_NAME   "ext_link"
#define GET_LINK_INFO_TEST_DSET_NAME       "get_link_info_dset"

#define GET_LINK_NAME_TEST_DSET_SPACE_RANK 2
#define GET_LINK_NAME_TEST_SUBGROUP_NAME   "get_link_name_test"
#define GET_LINK_NAME_TEST_DSET_NAME       "get_link_name_dset"

#define GET_LINK_VAL_TEST_SUBGROUP_NAME  "get_link_val_test"
#define GET_LINK_VAL_TEST_SOFT_LINK_NAME "soft_link"
#define GET_LINK_VAL_TEST_EXT_LINK_NAME  "ext_link"

#define LINK_ITER_TEST_DSET_SPACE_RANK 2
#define LINK_ITER_TEST_HARD_LINK_NAME  "link_iter_test_dset"
#define LINK_ITER_TEST_SOFT_LINK_NAME  "soft_link1"
#define LINK_ITER_TEST_EXT_LINK_NAME   "ext_link1"
#define LINK_ITER_TEST_SUBGROUP_NAME   "link_iter_test"
#define LINK_ITER_TEST_NUM_LINKS       3

#define LINK_ITER_TEST_0_LINKS_SUBGROUP_NAME "link_iter_test_0_links"

#define LINK_VISIT_TEST_NO_CYCLE_DSET_SPACE_RANK 2
#define LINK_VISIT_TEST_NO_CYCLE_DSET_NAME       "dset"
#define LINK_VISIT_TEST_NO_CYCLE_SUBGROUP_NAME   "link_visit_test_no_cycles"
#define LINK_VISIT_TEST_NO_CYCLE_SUBGROUP_NAME2  "link_visit_subgroup1"
#define LINK_VISIT_TEST_NO_CYCLE_SUBGROUP_NAME3  "link_visit_subgroup2"
#define LINK_VISIT_TEST_NO_CYCLE_LINK_NAME1      "hard_link1"
#define LINK_VISIT_TEST_NO_CYCLE_LINK_NAME2      "soft_link1"
#define LINK_VISIT_TEST_NO_CYCLE_LINK_NAME3      "ext_link1"
#define LINK_VISIT_TEST_NO_CYCLE_LINK_NAME4      "hard_link2"

#define LINK_VISIT_TEST_CYCLE_SUBGROUP_NAME  "link_visit_test_cycles"
#define LINK_VISIT_TEST_CYCLE_SUBGROUP_NAME2 "link_visit_subgroup1"
#define LINK_VISIT_TEST_CYCLE_SUBGROUP_NAME3 "link_visit_subgroup2"
#define LINK_VISIT_TEST_CYCLE_LINK_NAME1     "hard_link1"
#define LINK_VISIT_TEST_CYCLE_LINK_NAME2     "soft_link1"
#define LINK_VISIT_TEST_CYCLE_LINK_NAME3     "ext_link1"
#define LINK_VISIT_TEST_CYCLE_LINK_NAME4     "hard_link2"

#define LINK_VISIT_TEST_0_LINKS_SUBGROUP_NAME   "link_visit_test_0_links"
#define LINK_VISIT_TEST_0_LINKS_SUBGROUP_NAME2  "link_visit_test_0_links_subgroup1"
#define LINK_VISIT_TEST_0_LINKS_SUBGROUP_NAME3  "link_visit_test_0_links_subgroup2"


/*****************************************************
 *                                                   *
 *            Plugin Object test defines             *
 *                                                   *
 *****************************************************/

#define GENERIC_DATASET_OPEN_TEST_SPACE_RANK 2
#define GENERIC_DATASET_OPEN_TEST_DSET_NAME  "generic_dataset_open_test"

#define GENERIC_GROUP_OPEN_TEST_GROUP_NAME "generic_group_open_test"

#define GENERIC_DATATYPE_OPEN_TEST_TYPE_NAME "generic_datatype_open_test"

#define OBJECT_EXISTS_TEST_DSET_SPACE_RANK 2
#define OBJECT_EXISTS_TEST_SUBGROUP_NAME   "h5o_exists_by_name_test"
#define OBJECT_EXISTS_TEST_DTYPE_NAME      "h5o_exists_by_name_dtype"
#define OBJECT_EXISTS_TEST_DSET_NAME       "h5o_exists_by_name_dset"

#define OBJECT_COPY_TEST_SUBGROUP_NAME "object_copy_test"
#define OBJECT_COPY_TEST_SPACE_RANK    2
#define OBJECT_COPY_TEST_DSET_DTYPE    H5T_NATIVE_INT
#define OBJECT_COPY_TEST_DSET_NAME     "dset"
#define OBJECT_COPY_TEST_DSET_NAME2    "dset_copy"

#define H5O_CLOSE_TEST_SPACE_RANK 2
#define H5O_CLOSE_TEST_DSET_NAME  "h5o_close_test_dset"
#define H5O_CLOSE_TEST_TYPE_NAME  "h5o_close_test_type"

#define OBJ_REF_GET_TYPE_TEST_SUBGROUP_NAME "obj_ref_get_obj_type_test"
#define OBJ_REF_GET_TYPE_TEST_DSET_NAME "ref_dset"
#define OBJ_REF_GET_TYPE_TEST_TYPE_NAME "ref_dtype"
#define OBJ_REF_GET_TYPE_TEST_SPACE_RANK 2

#define OBJ_REF_DATASET_WRITE_TEST_SUBGROUP_NAME  "obj_ref_write_test"
#define OBJ_REF_DATASET_WRITE_TEST_REF_DSET_NAME  "ref_dset"
#define OBJ_REF_DATASET_WRITE_TEST_REF_TYPE_NAME  "ref_dtype"
#define OBJ_REF_DATASET_WRITE_TEST_SPACE_RANK     1
#define OBJ_REF_DATASET_WRITE_TEST_DSET_NAME      "obj_ref_dset"

#define OBJ_REF_DATASET_READ_TEST_SUBGROUP_NAME  "obj_ref_read_test"
#define OBJ_REF_DATASET_READ_TEST_REF_DSET_NAME  "ref_dset"
#define OBJ_REF_DATASET_READ_TEST_REF_TYPE_NAME  "ref_dtype"
#define OBJ_REF_DATASET_READ_TEST_SPACE_RANK     1
#define OBJ_REF_DATASET_READ_TEST_DSET_NAME      "obj_ref_dset"

#define OBJ_REF_DATASET_EMPTY_WRITE_TEST_SUBGROUP_NAME  "obj_ref_empty_write_test"
#define OBJ_REF_DATASET_EMPTY_WRITE_TEST_SPACE_RANK     1
#define OBJ_REF_DATASET_EMPTY_WRITE_TEST_DSET_NAME      "obj_ref_dset"


/*****************************************************
 *                                                   *
 *         Plugin Miscellaneous test defines         *
 *                                                   *
 *****************************************************/

#define OPEN_LINK_WITHOUT_SLASH_DSET_SPACE_RANK 2
#define OPEN_LINK_WITHOUT_SLASH_DSET_NAME       "link_without_slash_test_dset"

#define OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_CONTAINER_GROUP_NAME "absolute_path_test_container_group"
#define OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_SUBGROUP_NAME        "absolute_path_test_subgroup"
#define OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_DTYPE_NAME           "absolute_path_test_dtype"
#define OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_DSET_NAME            "absolute_path_test_dset"
#define OBJECT_CREATE_BY_ABSOLUTE_PATH_TEST_DSET_SPACE_RANK      3

#define ABSOLUTE_VS_RELATIVE_PATH_TEST_CONTAINER_GROUP_NAME "absolute_vs_relative_test_container_group"
#define ABSOLUTE_VS_RELATIVE_PATH_TEST_DSET1_NAME           "absolute_vs_relative_test_dset1"
#define ABSOLUTE_VS_RELATIVE_PATH_TEST_DSET2_NAME           "absolute_vs_relative_test_dset2"
#define ABSOLUTE_VS_RELATIVE_PATH_TEST_DSET3_NAME           "absolute_vs_relative_test_dset3"
#define ABSOLUTE_VS_RELATIVE_PATH_TEST_DSET4_NAME           "absolute_vs_relative_test_dset4"
#define ABSOLUTE_VS_RELATIVE_PATH_TEST_DSET5_NAME           "absolute_vs_relative_test_dset5"
#define ABSOLUTE_VS_RELATIVE_PATH_TEST_DSET6_NAME           "absolute_vs_relative_test_dset6"
#define ABSOLUTE_VS_RELATIVE_PATH_TEST_DSET_SPACE_RANK      3

#define URL_ENCODING_TEST_SPACE_RANK 2
#define URL_ENCODING_TEST_GROUP_NAME "url_encoding_group !*'():@&=+$,?#[]-.<>\\\\^`{}|~"
#define URL_ENCODING_TEST_DSET_NAME  "url_encoding_dset !*'():@&=+$,?#[]-.<>\\\\^`{}|~"
#define URL_ENCODING_TEST_ATTR_NAME  "url_encoding_attr !*'():@&=+$,?#[]-.<>\\\\^`{}|~"

#define COMPOUND_WITH_SYMBOLS_IN_MEMBER_NAMES_TEST_SUBGROUP_NAME "compound_type_with_symbols_in_member_names_test"
#define COMPOUND_WITH_SYMBOLS_IN_MEMBER_NAMES_TEST_NUM_SUBTYPES  9
#define COMPOUND_WITH_SYMBOLS_IN_MEMBER_NAMES_TEST_DSET_RANK     2
#define COMPOUND_WITH_SYMBOLS_IN_MEMBER_NAMES_TEST_DSET_NAME     "dset"


/* Error handling macros for the DAOS VOL test suite */

/* Use FUNC to safely handle variations of C99 __func__ keyword handling */
#ifdef H5_HAVE_C99_FUNC
#define FUNC __func__
#elif defined(H5_HAVE_FUNCTION)
#define FUNC __FUNCTION__
#else
#error "We need __func__ or __FUNCTION__ to test function names!"
#endif

/*
 * Print the current location on the standard output stream.
 */
#define AT()     if (MAINPROCESS) printf ("   at %s:%d in %s()...\n",        \
        __FILE__, __LINE__, FUNC);


/*
 * The name of the test is printed by saying TESTING("something") which will
 * result in the string `Testing something' being flushed to standard output.
 * If a test passes, fails, or is skipped then the PASSED(), H5_FAILED(), or
 * SKIPPED() macro should be called.  After H5_FAILED() or SKIPPED() the caller
 * should print additional information to stdout indented by at least four
 * spaces.
 */
#define TESTING(S)  {if (MAINPROCESS) printf("Testing %-66s", S); fflush(stdout);}
#define PASSED()    {if (MAINPROCESS) puts("PASSED"); fflush(stdout);}
#define H5_FAILED() {if (MAINPROCESS) puts("*FAILED*"); fflush(stdout);}
#define SKIPPED()   {if (MAINPROCESS) puts("- SKIPPED -"); fflush(stdout);}

#define TEST_ERROR  {H5_FAILED(); AT(); goto error;}

#endif
