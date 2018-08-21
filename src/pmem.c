/*
 * Copyright (c) 2017, Andreas Bluemle <andreas dot bluemle at itxperts dot de>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifdef USE_PMDK
#include <stdlib.h>
#include "server.h"
#include "obj.h"
#include "libpmemobj.h"
#include "util.h"
#include "sds.h"

int pmemReconstruct(void) {
    TOID(struct redis_pmem_root) root;
    TOID(struct key_val_pair_PM) pmem_toid;
    struct key_val_pair_PM *pmem_obj;
    dict *d;
    void *key;
    void *val;
    void *pmem_base_addr;

    root = server.pm_rootoid;
    pmem_base_addr = (void *)server.pm_pool->addr;
    d = server.db[0].dict;
    dictExpand(d, D_RO(root)->num_dict_entries);

    for (pmem_toid = D_RO(root)->pe_first;
        TOID_IS_NULL(pmem_toid) == 0;
        pmem_toid = D_RO(pmem_toid)->pmem_list_next
    ) {
		pmem_obj = (key_val_pair_PM *)(pmem_toid.oid.off + (uint64_t)pmem_base_addr);
		key = (void *)(pmem_obj->key_oid.off + (uint64_t)pmem_base_addr);
		val = (void *)(pmem_obj->val_oid.off + (uint64_t)pmem_base_addr);
        (void) dictAddReconstructedPM(d, key, val);
    }
    return C_OK;
}

#ifdef TODIS
int pmemReconstructTODIS(void) {
    serverLog(LL_TODIS, "   ");
    serverLog(LL_TODIS, "TODIS, pmemReconstructTODIS START");
    TOID(struct redis_pmem_root) root;
    TOID(struct key_val_pair_PM) pmem_toid;
    struct key_val_pair_PM *pmem_obj;
    dict *d;
    void *key;
    void *val;
    void *pmem_base_addr;

    root = server.pm_rootoid;
    pmem_base_addr = (void *)server.pm_pool->addr;
    d = server.db[0].dict;
    dictExpand(d, D_RO(root)->num_dict_entries + D_RO(root)->num_victim_entries);

    /* Reconstruct victim lists */
    for (pmem_toid = D_RO(root)->victim_first;
        TOID_IS_NULL(pmem_toid) == 0;
        pmem_toid = D_RO(pmem_toid)->pmem_list_next
    ) {
        pmem_obj = (key_val_pair_PM *)(pmem_toid.oid.off + (uint64_t)pmem_base_addr);
        key = (void *)(pmem_obj->key_oid.off + (uint64_t)pmem_base_addr);
        val = (void *)(pmem_obj->val_oid.off + (uint64_t)pmem_base_addr);

        robj *key_obj = createStringObject(sdsdup(key), sdslen(key));
        robj *val_obj = createStringObject(sdsdup(val), sdslen(val));
        dbReconstructVictim(&server.db[0], key_obj, val_obj);
    }
    /* Flush append only file (hard call)
     * This will remove all victim list... */
    forceFlushAppendOnlyFileTODIS();

    /* Reconstruct pmem lists */
    pmem_toid = D_RO(root)->pe_first;
    if (TOID_IS_NULL(pmem_toid)) {
        serverLog(LL_TODIS, "TODIS, pmemReconstruct END");
        return C_OK;
    }
    while (1) {
        pmem_obj = (key_val_pair_PM *)(
                pmem_toid.oid.off + (uint64_t)pmem_base_addr
        );
		key = (void *)(pmem_obj->key_oid.off + (uint64_t)pmem_base_addr);
		val = (void *)(pmem_obj->val_oid.off + (uint64_t)pmem_base_addr);

        server.used_pmem_memory += sdsAllocSizePM(key);
        server.used_pmem_memory += sdsAllocSizePM(val);
        server.used_pmem_memory += sizeof(struct key_val_pair_PM);
        serverLog(
                LL_TODIS,
                "TODIS, pmemReconstruct reconstructed used_pmem_memory: %zu",
                server.used_pmem_memory);

        dictAddReconstructedPM(d, key, val);
        if (TOID_EQUALS(pmem_toid, D_RO(root)->pe_last)) {
            break;
        }
        pmem_toid = D_RO(pmem_toid)->pmem_list_next;
    }

    serverLog(LL_TODIS, "TODIS, pmemReconstruct END");
    return C_OK;
}
#endif

void pmemKVpairSet(void *key, void *val)
{
    PMEMoid *pmem_oid_ptr;
    PMEMoid val_oid;
    struct key_val_pair_PM *pmem_obj;

    pmem_oid_ptr = sdsPMEMoidBackReference((sds)key);
    pmem_obj = (struct key_val_pair_PM *)pmemobj_direct(*pmem_oid_ptr);

    val_oid.pool_uuid_lo = server.pool_uuid_lo;
    val_oid.off = (uint64_t)val - (uint64_t)server.pm_pool->addr;

    TX_ADD_FIELD_DIRECT(pmem_obj, val_oid);
    pmem_obj->val_oid = val_oid;
    return;
}

#ifdef TODIS
void pmemKVpairSetRearrangeList(void *key, void *val)
{
    serverLog(LL_TODIS, "   ");
    serverLog(LL_TODIS, "TODIS, pmemKVpairSetRearrangeList START");
    PMEMoid *pmem_oid_ptr;

    pmem_oid_ptr = sdsPMEMoidBackReference((sds)key);
    pmemRemoveFromPmemList(*pmem_oid_ptr);
    PMEMoid new_pmem_oid = pmemAddToPmemList(key, val);
    *pmem_oid_ptr = new_pmem_oid;
    serverLog(LL_TODIS, "TODIS, pmemKVpairSetRearrangeList END");
    return;
}
#endif

PMEMoid
pmemAddToPmemList(void *key, void *val)
{
    serverLog(LL_TODIS, "   ");
    serverLog(LL_TODIS, "TODIS, pmemAddToPmemList START");
    PMEMoid key_oid;
    PMEMoid val_oid;
    PMEMoid pmem_oid;
    struct key_val_pair_PM *pmem_obj;
    TOID(struct key_val_pair_PM) pmem_toid;
    struct redis_pmem_root *root;

    key_oid.pool_uuid_lo = server.pool_uuid_lo;
    key_oid.off = (uint64_t)key - (uint64_t)server.pm_pool->addr;

    val_oid.pool_uuid_lo = server.pool_uuid_lo;
    val_oid.off = (uint64_t)val - (uint64_t)server.pm_pool->addr;

    pmem_oid = pmemobj_tx_zalloc(sizeof(struct key_val_pair_PM), pm_type_key_val_pair_PM);
    pmem_obj = (struct key_val_pair_PM *)pmemobj_direct(pmem_oid);
    pmem_obj->key_oid = key_oid;
    pmem_obj->val_oid = val_oid;
    pmem_toid.oid = pmem_oid;

#ifdef TODIS
        serverLog(
                LL_TODIS,
                "TODIS, pmemAddToPmemList key_val_pair_PM size: %zu",
                sizeof(struct key_val_pair_PM));
    server.used_pmem_memory += sizeof(struct key_val_pair_PM);
#endif

    root = pmemobj_direct(server.pm_rootoid.oid);

    pmem_obj->pmem_list_next = root->pe_first;
    if (!TOID_IS_NULL(root->pe_first)) {
        struct key_val_pair_PM *head = D_RW(root->pe_first);
        TX_ADD_FIELD_DIRECT(head,pmem_list_prev);
        head->pmem_list_prev = pmem_toid;
    }

    if (TOID_IS_NULL(root->pe_last)) {
        root->pe_last = pmem_toid;
    }

    TX_ADD_DIRECT(root);
    root->pe_first = pmem_toid;
    root->num_dict_entries++;

#ifdef TODIS
    serverLog(LL_TODIS, "TODIS, pmemAddFromPmemList END");
#endif
    return pmem_oid;
}

void
pmemRemoveFromPmemList(PMEMoid oid) {
#ifdef TODIS
    serverLog(LL_TODIS, "   ");
    serverLog(LL_TODIS, "TODIS, pmemRemoveFromPmemList START");
#endif
    TOID(struct key_val_pair_PM) pmem_toid;
    struct redis_pmem_root *root;

    root = pmemobj_direct(server.pm_rootoid.oid);

    pmem_toid.oid = oid;

#ifdef TODIS
        serverLog(
                LL_TODIS,
                "TODIS, pmemRemoveFromPmemList key_val_pair_PM size: %zu",
                sizeof(struct key_val_pair_PM));
        server.used_pmem_memory -= sizeof(struct key_val_pair_PM);
#endif

    if (TOID_EQUALS(root->pe_first, pmem_toid) &&
        TOID_EQUALS(root->pe_last, pmem_toid)) {
        TX_FREE(root->pe_first);
        TX_ADD_DIRECT(root);
        root->pe_first = TOID_NULL(struct key_val_pair_PM);
        root->pe_last = TOID_NULL(struct key_val_pair_PM);
    }
    else if(TOID_EQUALS(root->pe_first, pmem_toid)) {
        TOID(struct key_val_pair_PM) pmem_toid_next = D_RO(pmem_toid)->pmem_list_next;
        if(!TOID_IS_NULL(pmem_toid_next)){
            struct key_val_pair_PM *next = D_RW(pmem_toid_next);
            TX_ADD_FIELD_DIRECT(next,pmem_list_prev);
            next->pmem_list_prev.oid = OID_NULL;
        }
        TX_FREE(root->pe_first);
        TX_ADD_DIRECT(root);
        root->pe_first = pmem_toid_next;
    }
    else if (TOID_EQUALS(root->pe_last, pmem_toid)) {
        TOID(struct key_val_pair_PM) pmem_toid_prev = D_RO(pmem_toid)->pmem_list_prev;
        if (!TOID_IS_NULL(pmem_toid_prev)) {
            struct key_val_pair_PM *prev = D_RW(pmem_toid_prev);
            TX_ADD_FIELD_DIRECT(prev, pmem_list_next);
            prev->pmem_list_next.oid = OID_NULL;
        }
        TX_FREE(root->pe_last);
        TX_ADD_DIRECT(root);
        root->pe_last = pmem_toid_prev;
    }
    else {
        TOID(struct key_val_pair_PM) pmem_toid_prev = D_RO(pmem_toid)->pmem_list_prev;
        TOID(struct key_val_pair_PM) pmem_toid_next = D_RO(pmem_toid)->pmem_list_next;
        if(!TOID_IS_NULL(pmem_toid_prev)){
            struct key_val_pair_PM *prev = D_RW(pmem_toid_prev);
            TX_ADD_FIELD_DIRECT(prev, pmem_list_next);
            prev->pmem_list_next = pmem_toid_next;
        }
        if(!TOID_IS_NULL(pmem_toid_next)){
            struct key_val_pair_PM *next = D_RW(pmem_toid_next);
            TX_ADD_FIELD_DIRECT(next, pmem_list_prev);
            next->pmem_list_prev = pmem_toid_prev;
        }
        TX_FREE(pmem_toid);
    }
    TX_ADD_FIELD_DIRECT(root,num_dict_entries);
    root->num_dict_entries--;
    serverLog(LL_TODIS, "TODIS, pmemRemovFromPmemList END");
}
#endif

#ifdef TODIS
PMEMoid pmemUnlinkFromPmemList(PMEMoid oid) {
    serverLog(LL_TODIS, "   ");
    serverLog(LL_TODIS, "TODIS, pmemUnlinkFromPmemList START");
    TOID(struct key_val_pair_PM) pmem_toid;
    struct redis_pmem_root *root;

    root = pmemobj_direct(server.pm_rootoid.oid);
    pmem_toid.oid = oid;

    if (TOID_EQUALS(root->pe_first, pmem_toid) &&
        TOID_EQUALS(root->pe_last, pmem_toid)) {
        TX_ADD_DIRECT(root);
        root->pe_first = TOID_NULL(struct key_val_pair_PM);
        root->pe_last = TOID_NULL(struct key_val_pair_PM);
    }
    else if(TOID_EQUALS(root->pe_first, pmem_toid)) {
        TOID(struct key_val_pair_PM) pmem_toid_next = D_RO(pmem_toid)->pmem_list_next;
        if(!TOID_IS_NULL(pmem_toid_next)){
            struct key_val_pair_PM *next = D_RW(pmem_toid_next);
            TX_ADD_FIELD_DIRECT(next,pmem_list_prev);
            next->pmem_list_prev.oid = OID_NULL;
        }
        TX_ADD_DIRECT(root);
        root->pe_first = pmem_toid_next;
    }
    else if (TOID_EQUALS(root->pe_last, pmem_toid)) {
        TOID(struct key_val_pair_PM) pmem_toid_prev = D_RO(pmem_toid)->pmem_list_prev;
        if (!TOID_IS_NULL(pmem_toid_prev)) {
            struct key_val_pair_PM *prev = D_RW(pmem_toid_prev);
            TX_ADD_FIELD_DIRECT(prev, pmem_list_next);
            prev->pmem_list_next.oid = OID_NULL;
        }
        TX_ADD_DIRECT(root);
        root->pe_last = pmem_toid_prev;
    }
    else {
        TOID(struct key_val_pair_PM) pmem_toid_prev = D_RO(pmem_toid)->pmem_list_prev;
        TOID(struct key_val_pair_PM) pmem_toid_next = D_RO(pmem_toid)->pmem_list_next;
        if(!TOID_IS_NULL(pmem_toid_prev)){
            struct key_val_pair_PM *prev = D_RW(pmem_toid_prev);
            TX_ADD_FIELD_DIRECT(prev,pmem_list_next);
            prev->pmem_list_next = pmem_toid_next;
        }
        if(!TOID_IS_NULL(pmem_toid_next)){
            struct key_val_pair_PM *next = D_RW(pmem_toid_next);
            TX_ADD_FIELD_DIRECT(next,pmem_list_prev);
            next->pmem_list_prev = pmem_toid_prev;
        }
    }
    TX_ADD_FIELD_DIRECT(root,num_dict_entries);
    root->num_dict_entries--;
    serverLog(LL_TODIS, "TODIS, pmemUnlinkFromPmemList END");
    return oid;

}
#endif

#ifdef TODIS
PMEMoid getPmemKvOid(uint64_t i) {
    TOID(struct redis_pmem_root) root;
    TOID(struct key_val_pair_PM) pmem_toid;
    struct redis_pmem_root *root_obj;
    uint64_t count = 0;

    root = server.pm_rootoid;
    root_obj = pmemobj_direct(root.oid);

    if (i >= root_obj->num_dict_entries) return OID_NULL;

    for (pmem_toid = D_RO(root)->pe_first;
        TOID_IS_NULL(pmem_toid) == 0;
        pmem_toid = D_RO(pmem_toid)->pmem_list_next
    ) {
        if (count < i) {
            ++count;
            continue;
        }
        return pmem_toid.oid;
    }
    return OID_NULL;
}
#endif

#ifdef TODIS
void knuthUniqNumberGenerator(int *numbers, const int size, const int range) {
    int size_iter = 0;

    for (
        int range_iter = 0;
        range_iter < range && size_iter < size;
        ++range_iter
    ) {
        int random_range = range - range_iter;
        int random_size = size - size_iter;
        if (random() % random_range < random_size)
            numbers[size_iter++] = range_iter;
    }
}
#endif

#ifdef TODIS
/**
 * Getting best eviction PMEMoid function.
 * parameters
 * - victim_oids: Victim PMEMoids that filled by this function.
 */
int getBestEvictionKeysPMEMoid(PMEMoid *victim_oids) {
    TOID(struct redis_pmem_root) root;
    struct redis_pmem_root *root_obj;
    uint64_t num_pmem_entries;

    root = server.pm_rootoid;
    root_obj = pmemobj_direct(root.oid);
    num_pmem_entries = root_obj->num_dict_entries;

    // TODO(totoro): Improves overflow case that performance not bind to this
    // useless loop...
    if (server.pmem_victim_count > num_pmem_entries) {
        serverLog(
                LL_TODIS,
                "TODIS_ERROR, Number of victim count is larger than pmem entries!");
    }

    /* allkeys-random policy */
    if (server.max_pmem_memory_policy == MAXMEMORY_ALLKEYS_RANDOM) {
        // TODO(totoro): Implements bulk victim algorithm for RANDOM policy...
        /*int *indexes = zmalloc(sizeof(int) * server.pmem_victim_count);*/
        /*knuthUniqNumberGenerator(*/
                /*indexes, server.pmem_victim_count, num_pmem_entries);*/
        /*for (size_t i = 0; i < server.pmem_victim_count; ++i) {*/
            /*victim_oids[i] = getPmemKvOid(indexes[i]);*/
        /*}*/
        /*zfree(indexes);*/
        return C_ERR;
    }

    /* allkeys-lru policy */
    else if (server.max_pmem_memory_policy == MAXMEMORY_ALLKEYS_LRU) {
        TOID(struct key_val_pair_PM) victim_toid = root_obj->pe_last;
        for (int i = server.pmem_victim_count - 1; i >= 0; --i) {
            size_t count = server.pmem_victim_count - i;
            if (count > num_pmem_entries) {
                victim_oids[i] = OID_NULL;
                continue;
            }
            victim_oids[i] = victim_toid.oid;
            victim_toid = D_RO(victim_toid)->pmem_list_prev;
        }
        return C_OK;
    }
    return C_ERR;
}
#endif

#ifdef TODIS
PMEMoid getBestEvictionKeyPMEMoid(void) {
    TOID(struct redis_pmem_root) root;
    struct redis_pmem_root *root_obj;
    uint64_t num_pmem_entries;

    root = server.pm_rootoid;
    root_obj = pmemobj_direct(root.oid);
    num_pmem_entries = root_obj->num_dict_entries;

    /* allkeys-random policy */
    if (server.max_pmem_memory_policy == MAXMEMORY_ALLKEYS_RANDOM) {
        uint64_t index = random() % num_pmem_entries;
        return getPmemKvOid(index);
    }

    /* allkeys-lru policy */
    else if (server.max_pmem_memory_policy == MAXMEMORY_ALLKEYS_LRU) {
        TOID(struct key_val_pair_PM) victim_toid = root_obj->pe_last;
        if (TOID_IS_NULL(victim_toid)) {
            // There is no eviction victim in pmem...
            return OID_NULL;
        }

        return victim_toid.oid;
    }
    return OID_NULL;
}
#endif

#ifdef TODIS
struct key_val_pair_PM *getBestEvictionPMObject(void) {
    PMEMoid victim_oid = getBestEvictionKeyPMEMoid();

    if (OID_IS_NULL(victim_oid))
        return NULL;

    return getPMObjectFromOid(victim_oid);
}
#endif

#ifdef TODIS
sds getBestEvictionKeyPM(void) {
    struct key_val_pair_PM *victim_obj = getBestEvictionPMObject();

    if (victim_obj == NULL)
        return NULL;

    return getKeyFromPMObject(victim_obj);
}
#endif

#ifdef TODIS
struct key_val_pair_PM *getPMObjectFromOid(PMEMoid oid) {
    void *pmem_base_addr = (void *)server.pm_pool->addr;

    if (OID_IS_NULL(oid))
        return NULL;

    return (key_val_pair_PM *)(oid.off + (uint64_t) pmem_base_addr);
}
#endif

#ifdef TODIS
sds getKeyFromPMObject(struct key_val_pair_PM *obj) {
    void *pmem_base_addr = (void *)server.pm_pool->addr;

    if (obj == NULL)
        return NULL;

    return (sds)(obj->key_oid.off + (uint64_t) pmem_base_addr);
}
#endif

#ifdef TODIS
sds getValFromPMObject(struct key_val_pair_PM *obj) {
    void *pmem_base_addr = (void *)server.pm_pool->addr;

    if (obj == NULL)
        return NULL;

    return (sds)(obj->val_oid.off + (uint64_t) pmem_base_addr);
}
#endif

#ifdef TODIS
sds getKeyFromOid(PMEMoid oid) {
    struct key_val_pair_PM *obj = getPMObjectFromOid(oid);
    return getKeyFromPMObject(obj);
}
#endif

#ifdef TODIS
sds getValFromOid(PMEMoid oid) {
    struct key_val_pair_PM *obj = getPMObjectFromOid(oid);
    return getValFromPMObject(obj);
}
#endif

#ifdef TODIS
int evictPmemNodesToVictimList(PMEMoid *victim_oids) {
    serverLog(LL_TODIS, "   ");
    serverLog(LL_TODIS, "TODIS, evictPmemNodesToVictimList START");
    struct redis_pmem_root *root = pmemobj_direct(server.pm_rootoid.oid);

    TOID(struct key_val_pair_PM) start_toid = TOID_NULL(struct key_val_pair_PM);

    if (server.max_pmem_memory_policy == MAXMEMORY_ALLKEYS_RANDOM) {
        // TODO(totoro): Implements eviction node algorithm for RAMDOM policy...
        /*for (size_t i = 0; i < server.pmem_victim_count; ++i) {*/
            /*TOID(struct key_val_pair_PM) victim_toid;*/
            /*TOID(struct key_val_pair_PM) victim_legacy_root_toid;*/

            /*PMEMoid victim_oid = victim_oids[i];*/
            /*if (OID_IS_NULL(victim_oid)) {*/
                /*serverLog(LL_TODIS, "TODIS_ERROR, victim_oid is null");*/
                /*return C_ERR;*/
            /*}*/

            /*[> Adds victim node to Victim list. <]*/
            /*root = pmemobj_direct(server.pm_rootoid.oid);*/
            /*victim_obj = (struct key_val_pair_PM *)pmemobj_direct(victim_oid);*/
            /*victim_toid.oid = victim_oid;*/

            /*victim_legacy_root_toid = start;*/
            /*if (!TOID_IS_NULL(start)) {*/
                /*struct key_val_pair_PM *head = D_RW(start);*/
                /*TX_ADD_FIELD_DIRECT(head, pmem_list_prev);*/
                /*head->pmem_list_prev = victim_toid;*/
            /*}*/

            /*start = victim_toid;*/
            /*if (TOID_IS_NULL(end)) {*/
                /*end = victim_toid;*/
                /*struct key_val_pair_PM *rear = D_RW(end);*/
                /*TX_ADD_FIELD_DIRECT(rear, pmem_list_next);*/
                /*rear->pmem_list_next = root->victim_start;*/

                /*struct key_val_pair_PM *old_victim_head = D_RW(root->victim_start);*/
                /*TX_ADD_FIELD_DIRECT(old_victim_head, pmem_list_prev);*/
                /*old_victim_head->pmem_list_prev = end;*/
            /*}*/

            /*TX_ADD_DIRECT(root);*/
            /*root->victim_start = start;*/

            /*serverLog(LL_TODIS, "TODIS, victim key: %s", getKeyFromOid(victim_oid));*/

            /*[> Unlinks victim node from PMEM list. <]*/
            /*pmemUnlinkFromPmemList(victim_oid);*/
            /*victim_obj->pmem_list_next = victim_legacy_root_toid;*/
        /*}*/
        serverLog(LL_TODIS, "TODIS, evictPmemNodesToVictimList RANDOM END");
        return C_ERR;
    }
    else if (server.max_pmem_memory_policy == MAXMEMORY_ALLKEYS_LRU) {
        PMEMoid start_oid = OID_NULL;
        for (size_t i = 0; i < server.pmem_victim_count; ++i) {
            if (!OID_IS_NULL(victim_oids[i])) {
                start_oid = victim_oids[i];
                TX_ADD_DIRECT(root);
                root->num_dict_entries -= server.pmem_victim_count - i;
                root->num_victim_entries += server.pmem_victim_count - i;
                break;
            }
        }
        if (OID_IS_NULL(start_oid)) {
            serverLog(LL_TODIS, "TODIS_CRITICAL_ERROR, all of victim oids is null");
            return C_ERR;
        }
        start_toid.oid = start_oid;

        TX_ADD_FIELD_DIRECT(root, pe_last);
        root->victim_first = start_toid;
        root->pe_last = D_RO(start_toid)->pmem_list_prev;
        if (TOID_IS_NULL(root->pe_last)) {
            root->pe_first = TOID_NULL(struct key_val_pair_PM);
        }

        serverLog(LL_TODIS, "TODIS, evictPmemNodesToVictimList LRU END");
        return C_OK;
    }

    return C_ERR;
}
#endif

#ifdef TODIS
int evictPmemNodeToVictimList(PMEMoid victim_oid) {
    serverLog(LL_TODIS, "   ");
    serverLog(LL_TODIS, "TODIS, evictPmemNodeToVictimList START");
    TOID(struct key_val_pair_PM) victim_toid;
    TOID(struct key_val_pair_PM) victim_legacy_root_toid;
    struct redis_pmem_root *root;
    struct key_val_pair_PM *victim_obj;

    if (OID_IS_NULL(victim_oid)) {
        serverLog(LL_TODIS, "TODIS_ERROR, victim_oid is null");
        return C_ERR;
    }

    /* Adds victim node to Victim list. */
    root = pmemobj_direct(server.pm_rootoid.oid);
    victim_obj = (struct key_val_pair_PM *)pmemobj_direct(victim_oid);
    victim_toid.oid = victim_oid;

    victim_legacy_root_toid = root->victim_first;
    if (!TOID_IS_NULL(root->victim_first)) {
        struct key_val_pair_PM *head = D_RW(root->victim_first);
        TX_ADD_FIELD_DIRECT(head, pmem_list_prev);
        head->pmem_list_prev = victim_toid;
    }

    TX_ADD_DIRECT(root);
    root->victim_first = victim_toid;
    root->num_victim_entries++;

    serverLog(LL_TODIS, "TODIS, victim key: %s", getKeyFromOid(victim_oid));

    /* Unlinks victim node from PMEM list. */
    pmemUnlinkFromPmemList(victim_oid);
    victim_obj->pmem_list_next = victim_legacy_root_toid;
    serverLog(LL_TODIS, "TODIS, evictPmemNodeToVictimList END");
    return C_OK;
}
#endif

#ifdef TODIS
void freeVictim(PMEMoid oid) {
    sds key = getKeyFromOid(oid);
    sds val = getValFromOid(oid);
    sdsfreeVictim(key);
    sdsfreeVictim(val);
}
#endif

#ifdef TODIS
void freeVictimList(PMEMoid start_oid) {
    serverLog(LL_TODIS, "   ");
    serverLog(LL_TODIS, "TODIS, freeVictimList START");
    struct redis_pmem_root *root;

    root = pmemobj_direct(server.pm_rootoid.oid);
    TOID(struct key_val_pair_PM) victim_first_toid;
    victim_first_toid.oid = start_oid;

    if (OID_IS_NULL(start_oid)) {
        serverLog(LL_TODIS, "TODIS, freeVictimList END");
        return;
    }

    /* Unlinks victim list from another victim list. */
    if (TOID_EQUALS(root->victim_first, victim_first_toid)) {
        TX_ADD_DIRECT(root);
        root->victim_first = TOID_NULL(struct key_val_pair_PM);
    }
    TOID(struct key_val_pair_PM) prev_toid = D_RO(victim_first_toid)->pmem_list_prev;
    if (!TOID_IS_NULL(prev_toid)) {
        struct key_val_pair_PM *prev_obj = pmemobj_direct(prev_toid.oid);
        prev_obj->pmem_list_next = TOID_NULL(struct key_val_pair_PM);
    }

    /* Free all Victim list. */
    while (!TOID_IS_NULL(victim_first_toid)) {
        TOID(struct key_val_pair_PM) next_toid = D_RO(victim_first_toid)->pmem_list_next;
        freeVictim(victim_first_toid.oid);
        TX_FREE(victim_first_toid);
        TX_ADD_DIRECT(root);
        root->num_victim_entries--;
        victim_first_toid = next_toid;
    }
    serverLog(LL_TODIS, "TODIS, freeVictimList END");
}
#endif

#ifdef TODIS
size_t pmem_used_memory(void) {
    return server.used_pmem_memory;
}
#endif

#ifdef TODIS
size_t sizeOfPmemNode(PMEMoid oid) {
    size_t total_size = 0;

    sds key = getKeyFromOid(oid);
    sds val = getValFromOid(oid);
    size_t node_size = sizeof(struct key_val_pair_PM);

    total_size += sdsAllocSizePM(key);
    total_size += sdsAllocSizePM(val);
    total_size += node_size;
    serverLog(
            LL_TODIS,
            "TODIS, sizeOfPmemNode: %zu",
            total_size);

    return total_size;
}
#endif

#ifdef TODIS
struct redis_pmem_root *getPmemRootObject(void) {
    return pmemobj_direct(server.pm_rootoid.oid);
}
#endif
