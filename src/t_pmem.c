#include "server.h"
#if defined(TODIS) && defined(PMDK)
#include "pmem.h"
#endif

/*-----------------------------------------------------------------------------
 * Check PMEM / DRAM status command
 *----------------------------------------------------------------------------*/
#ifdef TODIS
void getPmemStatusCommand(client *c) {
    long long used_pmem_memory = (long long) pmem_used_memory();
    void *replylen = addDeferredMultiBulkLength(c);
    unsigned long numreplies = 0;

    dictIterator *di;
    dictEntry *de;

    addReplyBulkCString(c, "used pmem memory:");
    numreplies++;
    addReplyBulkLongLong(c, used_pmem_memory);
    numreplies++;

    addReplyBulkCString(c, "pmem entries:");
    numreplies++;
    addReplyBulkLongLong(c, dictSizePM(c->db->dict));
    numreplies++;

    di = dictGetSafeIterator(c->db->dict);
    while ((de = dictNext(di)) != NULL) {
        if (de->location == LOCATION_DRAM) continue;
        sds key = dictGetKey(de);
        robj *keyobj;

        keyobj = createStringObject(key, sdslen(key));
        if (expireIfNeeded(c->db, keyobj) == 0) {
            addReplyBulk(c, keyobj);
            numreplies++;
            addReplyBulk(c, dictGetVal(de));
            numreplies++;
        }
        decrRefCount(keyobj);
    }

    dictReleaseIterator(di);
    setDeferredMultiBulkLength(c, replylen, numreplies);
}

void getDramStatusCommand(client *c) {
    long long used_dram_memory = (long long) zmalloc_used_memory();
    void *replylen = addDeferredMultiBulkLength(c);
    unsigned long numreplies = 0;

    dictIterator *di;
    dictEntry *de;

    addReplyBulkCString(c, "used dram memory:");
    numreplies++;
    addReplyBulkLongLong(c, used_dram_memory);
    numreplies++;

    addReplyBulkCString(c, "dram entries:");
    numreplies++;
    addReplyBulkLongLong(c, dictSize(c->db->dict) - dictSizePM(c->db->dict));
    numreplies++;

    di = dictGetSafeIterator(c->db->dict);
    while ((de = dictNext(di)) != NULL) {
        if (de->location == LOCATION_PMEM) continue;
        sds key = dictGetKey(de);
        robj *keyobj;

        keyobj = createStringObject(key, sdslen(key));
        if (expireIfNeeded(c->db, keyobj) == 0) {
            addReplyBulk(c, keyobj);
            numreplies++;
            addReplyBulk(c, dictGetVal(de));
            numreplies++;
        }
        decrRefCount(keyobj);
    }

    dictReleaseIterator(di);
    setDeferredMultiBulkLength(c, replylen, numreplies);
}
#endif
