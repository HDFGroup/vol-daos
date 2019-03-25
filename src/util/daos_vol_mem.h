/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of the HDF5 DAOS VOL connector. The full copyright      *
 * notice, including terms governing use, modification, and redistribution,  *
 * is contained in the COPYING file, which can be found at the root of the   *
 * source code distribution tree.                                            *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

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
