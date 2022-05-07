/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "daos_vol_mem.h"

#ifdef DV_TRACK_MEM_USAGE
extern size_t daos_vol_curr_alloc_bytes;
#endif

/*-------------------------------------------------------------------------
 * Function:    DV_malloc
 *
 * Purpose:     Similar to the C89 version of malloc().
 *
 *              On size of 0, we return a NULL pointer instead of the
 *              standard-allowed 'special' pointer since that's more
 *              difficult to check as a return value. This is still
 *              considered an error condition since allocations of zero
 *              bytes usually indicate problems.
 *
 * Return:      Success:    Pointer to new memory
 *              Failure:    NULL
 *
 * Programmer:  Jordan Henderson
 *              Oct 16, 2018
 */
void *
DV_malloc(size_t size)
{
    void *ret_value = NULL;

    if (size) {
#ifdef DV_TRACK_MEM_USAGE
        size_t block_size = size;

        /* Keep track of the allocated size */
        if (NULL != (ret_value = malloc(size + sizeof(block_size)))) {
            memcpy(ret_value, &block_size, sizeof(block_size));
            ret_value = (char *)ret_value + sizeof(block_size);

            daos_vol_curr_alloc_bytes += size;
        } /* end if */
#else
        ret_value = malloc(size);
#endif
    } /* end if */
    else
        ret_value = NULL;

    return ret_value;
} /* end DV_malloc() */

/*-------------------------------------------------------------------------
 * Function:    DV_calloc
 *
 * Purpose:     Similar to the C89 version of calloc(), except this
 *              routine just takes a 'size' parameter.
 *
 *              On size of 0, we return a NULL pointer instead of the
 *              standard-allowed 'special' pointer since that's more
 *              difficult to check as a return value. This is still
 *              considered an error condition since allocations of zero
 *              bytes usually indicate problems.
 *
 *
 * Return:      Success:    Pointer to new memory
 *              Failure:    NULL
 *
 * Programmer:  Jordan Henderson
 *              Oct 16, 2018
 */
void *
DV_calloc(size_t size)
{
    void *ret_value = NULL;

    if (size) {
#ifdef DV_TRACK_MEM_USAGE
        if (NULL != (ret_value = DV_malloc(size)))
            memset(ret_value, 0, size);
#else
        ret_value = calloc(1, size);
#endif
    } /* end if */
    else
        ret_value = NULL;

    return ret_value;
} /* end DV_calloc() */

/*-------------------------------------------------------------------------
 * Function:    DV_realloc
 *
 * Purpose:     Similar semantics as C89's realloc(). Specifically, the
 *              following calls are equivalent:
 *
 *              DV_realloc(NULL, size)    <==> DV_malloc(size)
 *              DV_realloc(ptr, 0)        <==> DV_free(ptr)
 *              DV_realloc(NULL, 0)       <==> NULL
 *
 *              Note that the (NULL, 0) combination is undefined behavior
 *              in the C standard.
 *
 * Return:      Success:    Ptr to new memory if size > 0
 *                          NULL if size is zero
 *              Failure:    NULL (input buffer is unchanged on failure)
 *
 * Programmer:  Jordan Henderson
 *              Oct 16, 2018
 */
void *
DV_realloc(void *mem, size_t size)
{
    void *ret_value = NULL;

    if (!(NULL == mem && 0 == size)) {
#ifdef DV_TRACK_MEM_USAGE
        if (size > 0) {
            if (mem) {
                size_t block_size;

                memcpy(&block_size, (char *)mem - sizeof(block_size), sizeof(block_size));

                ret_value = DV_malloc(size);
                memcpy(ret_value, mem, size < block_size ? size : block_size);
                DV_free(mem);
            } /* end if */
            else
                ret_value = DV_malloc(size);
        } /* end if */
        else
            ret_value = DV_free(mem);
#else
        ret_value = realloc(mem, size);

        if (0 == size)
            ret_value = NULL;
#endif
    } /* end if */

    return ret_value;
} /* end DV_realloc() */

/*-------------------------------------------------------------------------
 * Function:    DV_free
 *
 * Purpose:     Just like free(3) except null pointers are allowed as
 *              arguments, and the return value (always NULL) can be
 *              assigned to the pointer whose memory was just freed:
 *
 *              thing = DV_free (thing);
 *
 * Return:      Success:    NULL
 *              Failure:    never fails
 *
 * Programmer:  Jordan Henderson
 *              Oct 16, 2018
 */
void *
DV_free(void *mem)
{
    if (mem) {
#ifdef DV_TRACK_MEM_USAGE
        size_t block_size;

        memcpy(&block_size, (char *)mem - sizeof(block_size), sizeof(block_size));
        daos_vol_curr_alloc_bytes -= block_size;

        free((char *)mem - sizeof(block_size));
#else
        free(mem);
#endif
    } /* end if */

    return NULL;
} /* end DV_free() */
