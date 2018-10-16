cmake_minimum_required (VERSION 3.1.0)
PROJECT (DAOS_VOL_UTIL C)

set (DAOS_VOL_SRCS
    ${DAOS_VOL_SRCS}
    ${DAOS_VOL_UTIL_SOURCE_DIR}/daos_vol_mem.c
    PARENT_SCOPE
)

set (DAOS_VOL_HDRS
    ${DAOS_VOL_HDRS}
    ${DAOS_VOL_UTIL_SOURCE_DIR}/daos_vol_mem.h
    PARENT_SCOPE
)