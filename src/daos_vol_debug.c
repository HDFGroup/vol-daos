/**
 * Copyright (c) 2018-2022 The HDF Group.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "daos_vol_private.h"

#include "util/daos_vol_err.h" /* DAOS connector error handling           */
#include "util/daos_vol_mem.h" /* DAOS connector memory management        */

#include <ctype.h>

#ifdef DV_PLUGIN_DEBUG

/*
 * Macro to loop over asking DAOS for a list of akeys/dkeys for an object
 * and stop as soon as at least one key is retrieved. If DAOS returns
 * -DER_KEY2BIG, the loop will re-allocate the specified key buffer as
 * necessary and try again. The variadic portion of this macro corresponds
 * to the arguments given to daos_obj_list_akey/dkey.
 */
#define H5_DAOS_RETRIEVE_KEYS_LOOP(key_buf, key_buf_len, sg_iov, nr, nr_init, maj_err, daos_obj_list_func,   \
                                   ...)                                                                      \
    do {                                                                                                     \
        /* Reset nr */                                                                                       \
        nr = nr_init;                                                                                        \
                                                                                                             \
        /* Ask DAOS for a list of keys, break out if we succeed */                                           \
        if (0 == (ret = daos_obj_list_func(__VA_ARGS__)))                                                    \
            break;                                                                                           \
                                                                                                             \
        /*                                                                                                   \
         * Call failed - if the buffer is too small double it and                                            \
         * try again, otherwise fail.                                                                        \
         */                                                                                                  \
        if (ret == -DER_KEY2BIG) {                                                                           \
            char *tmp_realloc;                                                                               \
                                                                                                             \
            /* Allocate larger buffer */                                                                     \
            key_buf_len *= 2;                                                                                \
            if (NULL == (tmp_realloc = (char *)DV_realloc(key_buf, key_buf_len)))                            \
                D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't reallocate key buffer");              \
            key_buf = tmp_realloc;                                                                           \
                                                                                                             \
            /* Update SGL */                                                                                 \
            daos_iov_set(&sg_iov, key_buf, (daos_size_t)(key_buf_len - 1));                                  \
        } /* end if */                                                                                       \
        else                                                                                                 \
            D_GOTO_ERROR(maj_err, H5E_CANTGET, FAIL, "can't list keys: %s", H5_daos_err_to_string(ret));     \
    } while (1)

/*-------------------------------------------------------------------------
 * Function:    H5_daos_dump_obj_keys
 *
 * Purpose:     Debugging routine to list all of the dkeys for an object
 *              and each of the akeys under those dkeys.
 *
 *              TODO: Handle printing of non-string data, like link
 *              creation order valued akeys
 *
 * Return:      Success:        SUCCEED
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_daos_dump_obj_keys(daos_handle_t obj)
{
    daos_sg_list_t dkey_sgl;
    daos_sg_list_t akey_sgl;
    daos_anchor_t  dkey_anchor;
    daos_iov_t     dkey_sg_iov;
    daos_iov_t     akey_sg_iov;
    uint32_t       dkey_nr, akey_nr;
    uint32_t       i, j;
    size_t         dkey_buf_len = 0;
    size_t         akey_buf_len = 0;
    char          *dkey_buf     = NULL;
    char          *akey_buf     = NULL;
    int            ret;
    herr_t         ret_value = SUCCEED;

    /* Allocate dkey_buf */
    if (NULL == (dkey_buf = (char *)DV_malloc(H5_DAOS_ITER_SIZE_INIT)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "failed to allocate key buffer");
    dkey_buf_len = H5_DAOS_ITER_SIZE_INIT;

    /* Set up sgl.  Report size as 1 less than buffer size so we
     * always have room for a null terminator. */
    daos_iov_set(&dkey_sg_iov, dkey_buf, (daos_size_t)(dkey_buf_len - 1));
    dkey_sgl.sg_nr     = 1;
    dkey_sgl.sg_nr_out = 0;
    dkey_sgl.sg_iovs   = &dkey_sg_iov;

    /* Allocate akey_buf */
    if (NULL == (akey_buf = (char *)DV_malloc(H5_DAOS_ITER_SIZE_INIT)))
        D_GOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "failed to allocate key buffer");
    akey_buf_len = H5_DAOS_ITER_SIZE_INIT;

    /* Set up sgl.  Report size as 1 less than buffer size so we
     * always have room for a null terminator. */
    daos_iov_set(&akey_sg_iov, akey_buf, (daos_size_t)(akey_buf_len - 1));
    akey_sgl.sg_nr     = 1;
    akey_sgl.sg_nr_out = 0;
    akey_sgl.sg_iovs   = &akey_sg_iov;

    printf("*-------------------------------------*\n");
    printf("|                                     |\n");
    printf("| Object handle %-21" PRIu64 " |\n", obj.cookie);
    printf("|                                     |\n");
    printf("*-------------------------------------*\n\n");

    /* Initialize dkey loop anchor */
    memset(&dkey_anchor, 0, sizeof(dkey_anchor));

    do {
        daos_key_desc_t dkey_desc[H5_DAOS_ITER_LEN];
        char           *dkey_p;

        /* Loop to retrieve keys (exit as soon as we get at least 1 key) */
        H5_DAOS_RETRIEVE_KEYS_LOOP(dkey_buf, dkey_buf_len, dkey_sg_iov, dkey_nr, H5_DAOS_ITER_LEN, H5E_VOL,
                                   daos_obj_list_dkey, obj, DAOS_TX_NONE, &dkey_nr, dkey_desc, &dkey_sgl,
                                   &dkey_anchor, NULL);

        /* Loop over returned keys */
        dkey_p = dkey_buf;
        for (i = 0; i < dkey_nr; i++) {
            daos_key_desc_t akey_desc[H5_DAOS_ITER_LEN];
            daos_anchor_t   akey_anchor;
            daos_key_t      dkey;
            char           *akey_p;
            char            tmp_char = '\0';

            /* Add null terminator temporarily */
            tmp_char                        = dkey_p[dkey_desc[i].kd_key_len];
            dkey_p[dkey_desc[i].kd_key_len] = '\0';

            printf("-- Dkey '%s' --\n", dkey_p);

            /* Initialize akey loop anchor */
            memset(&akey_anchor, 0, sizeof(akey_anchor));

            /* Set up dkey */
            daos_iov_set(&dkey, dkey_p, dkey_desc[i].kd_key_len);

            do {
                /* Loop to retrieve keys (exit as soon as we get at least 1 key) */
                H5_DAOS_RETRIEVE_KEYS_LOOP(akey_buf, akey_buf_len, akey_sg_iov, akey_nr, H5_DAOS_ITER_LEN,
                                           H5E_VOL, daos_obj_list_akey, obj, DAOS_TX_NONE, &dkey, &akey_nr,
                                           akey_desc, &akey_sgl, &akey_anchor, NULL);

                /* Loop over returned keys */
                akey_p = akey_buf;
                for (j = 0; j < akey_nr; j++) {
                    char tmp_char2 = '\0';

                    /* Add null terminator temporarily */
                    tmp_char2                       = akey_p[akey_desc[j].kd_key_len];
                    akey_p[akey_desc[j].kd_key_len] = '\0';

                    if (!isalnum(*akey_p) && (*akey_p != '/')) {
                        char               numeric_str_buf[1024];
                        unsigned long long value;

                        memcpy(&value, akey_p, (size_t)akey_desc[j].kd_key_len);

                        snprintf(numeric_str_buf, 1024, "%lld", value);

                        printf(" -> Akey '%s%s'\n", numeric_str_buf,
                               (akey_desc[j].kd_key_len == 9) ? "" : "-0");
                    } /* end if */
                    else
                        printf(" -> Akey '%s'\n", akey_p);

                    /* Replace null terminator */
                    akey_p[akey_desc[j].kd_key_len] = tmp_char2;

                    /* Advance to next akey */
                    akey_p += akey_desc[j].kd_key_len;
                } /* end for */
            } while (!daos_anchor_is_eof(&akey_anchor));

            /* Replace null terminator */
            dkey_p[dkey_desc[i].kd_key_len] = tmp_char;

            /* Advance to next dkey */
            dkey_p += dkey_desc[i].kd_key_len;
        } /* end for */
    } while (!daos_anchor_is_eof(&dkey_anchor));

    printf("\n\n");

done:
    akey_buf = DV_free(akey_buf);
    dkey_buf = DV_free(dkey_buf);

    D_FUNC_LEAVE;
} /* end H5_daos_dump_obj_keys() */

#endif /* DV_PLUGIN_DEBUG */
