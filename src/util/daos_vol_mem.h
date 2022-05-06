/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef daos_vol_mem_H
#define daos_vol_mem_H

#ifdef __cplusplus
extern "C" {
#endif

void *DV_malloc(size_t size);
void *DV_calloc(size_t size);
void *DV_realloc(void *mem, size_t size);
void *DV_free(void *mem);

#ifdef __cplusplus
}
#endif

#endif /* daos_vol_mem_H */
