#include "server.h"
#ifdef TODIS
#include "pmem.h"
#include "obj.h"
#include "libpmemobj.h"
#include "util.h"
#endif
#include <math.h> /* isnan(), isinf() */


/*-----------------------------------------------------------------------------
 * Aof Set Command
 *----------------------------------------------------------------------------*/

/* The setGenericCommand() function implements the SET operation with different
 * options and variants. This function is called in order to implement the
 * following commands: SET, SETEX, PSETEX, SETNX.
 *
 * 'flags' changes the behavior of the command (NX or XX, see belove).
 *
 * 'expire' represents an expire to set in form of a Redis object as passed
 * by the user. It is interpreted according to the specified 'unit'.
 *
 * 'ok_reply' and 'abort_reply' is what the function will reply to the client
 * if the operation is performed, or when it is not because of NX or
 * XX flags.
 *
 * If ok_reply is NULL "+OK" is used.
 * If abort_reply is NULL, "$-1" is used. */

#define OBJ_AOF_SET_NO_FLAGS 0
#define OBJ_AOF_SET_NX (1<<0)     /* Set if key not exists. */
#define OBJ_AOF_SET_XX (1<<1)     /* Set if key exists. */
#define OBJ_AOF_SET_EX (1<<2)     /* Set if time in seconds is given */
#define OBJ_AOF_SET_PX (1<<3)     /* Set if time in ms in given */

void aofSetGenericCommand(client *c, int flags, robj *key, robj *val, robj *expire, int unit, robj *ok_reply, robj *abort_reply) {
    long long milliseconds = 0; /* initialized to avoid any harmness warning */
    robj *convertedVal = val;

    if (expire) {
        if (getLongLongFromObjectOrReply(c, expire, &milliseconds, NULL) != C_OK)
            return;
        if (milliseconds <= 0) {
            addReplyErrorFormat(c,"invalid expire time in %s",c->cmd->name);
            return;
        }
        if (unit == UNIT_SECONDS) milliseconds *= 1000;
    }

    if ((flags & OBJ_AOF_SET_NX && lookupKeyWrite(c->db,key) != NULL) ||
        (flags & OBJ_AOF_SET_XX && lookupKeyWrite(c->db,key) == NULL))
    {
        addReply(c, abort_reply ? abort_reply : shared.nullbulk);
        return;
    }
#ifdef TODIS
    if (val->encoding == OBJ_ENCODING_INT) {
        char int_str_buf[1024];
        sprintf(int_str_buf, "%lld", val->ptr);
        convertedVal = createStringObject(int_str_buf, strlen(int_str_buf));
    }
#endif
    setKey(c->db, key, convertedVal);
    server.dirty++;
    if (expire) setExpire(c->db,key,mstime()+milliseconds);
    notifyKeyspaceEvent(NOTIFY_STRING,"set",key,c->db->id);
    if (expire) notifyKeyspaceEvent(NOTIFY_GENERIC,
        "expire",key,c->db->id);
    addReply(c, ok_reply ? ok_reply : shared.ok);
}

/* AOFSET key value [NX] [XX] [EX <seconds>] [PX <milliseconds>] */
void aofSetCommand(client *c) {
    int j;
    robj *expire = NULL;
    int unit = UNIT_SECONDS;
    int flags = OBJ_AOF_SET_NO_FLAGS;

    for (j = 3; j < c->argc; j++) {
        char *a = c->argv[j]->ptr;
        robj *next = (j == c->argc-1) ? NULL : c->argv[j+1];

        if ((a[0] == 'n' || a[0] == 'N') &&
            (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
            !(flags & OBJ_AOF_SET_XX))
        {
            flags |= OBJ_AOF_SET_NX;
        } else if ((a[0] == 'x' || a[0] == 'X') &&
                   (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
                   !(flags & OBJ_AOF_SET_NX))
        {
            flags |= OBJ_AOF_SET_XX;
        } else if ((a[0] == 'e' || a[0] == 'E') &&
                   (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
                   !(flags & OBJ_AOF_SET_PX) && next)
        {
            flags |= OBJ_AOF_SET_EX;
            unit = UNIT_SECONDS;
            expire = next;
            j++;
        } else if ((a[0] == 'p' || a[0] == 'P') &&
                   (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
                   !(flags & OBJ_AOF_SET_EX) && next)
        {
            flags |= OBJ_AOF_SET_PX;
            unit = UNIT_MILLISECONDS;
            expire = next;
            j++;
        } else {
            addReply(c,shared.syntaxerr);
            return;
        }
    }

    c->argv[2] = tryObjectEncoding(c->argv[2]);
    aofSetGenericCommand(c,flags,c->argv[1],c->argv[2],expire,unit,NULL,NULL);
}

/*-----------------------------------------------------------------------------
 * Check PMEM / DRAM status command
 *----------------------------------------------------------------------------*/
#ifdef TODIS
void getPmemProcessTimeCommand(client *c) {
    void *replylen = addDeferredMultiBulkLength(c);
    unsigned long numreplies = 0;

    addReplyBulkCString(c, "pmem used time for list process:");
    numreplies++;
    addReplyBulkLongLong(c, server.pmem_list_time);
    numreplies++;

    addReplyBulkCString(c, "total process used time:");
    numreplies++;
    addReplyBulkLongLong(c, server.process_time);
    numreplies++;

    addReplyBulkCString(c, "freePmemMemoryIfNeeded, 1) Find victim key:");
    numreplies++;
    addReplyBulkLongLong(c, server.free_pmem_time_find_victim_key);
    numreplies++;

    addReplyBulkCString(c, "freePmemMemoryIfNeeded, 2) Make dram key val:");
    numreplies++;
    addReplyBulkLongLong(c, server.free_pmem_time_make_dram_key_val);
    numreplies++;

    addReplyBulkCString(c, "freePmemMemoryIfNeeded, 3) Dict unlink:");
    numreplies++;
    addReplyBulkLongLong(c, server.free_pmem_time_dict_unlink);
    numreplies++;

    addReplyBulkCString(c, "freePmemMemoryIfNeeded, 4) Pmem eviction:");
    numreplies++;
    addReplyBulkLongLong(c, server.free_pmem_time_pmem_eviction);
    numreplies++;

    addReplyBulkCString(c, "freePmemMemoryIfNeeded, 5) Pmem entry free:");
    numreplies++;
    addReplyBulkLongLong(c, server.free_pmem_time_pmem_entry_free);
    numreplies++;

    addReplyBulkCString(c, "freePmemMemoryIfNeeded, 6) Add dram entry:");
    numreplies++;
    addReplyBulkLongLong(c, server.free_pmem_time_add_dram_entry);
    numreplies++;

    addReplyBulkCString(c, "freePmemMemoryIfNeeded, 7) Feed dram log:");
    numreplies++;
    addReplyBulkLongLong(c, server.free_pmem_time_feed_dram_log);
    numreplies++;

    setDeferredMultiBulkLength(c, replylen, numreplies);
}
#endif
#ifdef TODIS
void getPmemStatusCommand(client *c) {
    long long used_pmem_memory = (long long) pmem_used_memory();
    void *replylen = addDeferredMultiBulkLength(c);
    unsigned long numreplies = 0;
    char str_buf[1024];

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
        robj *valobj = dictGetVal(de);

        keyobj = createStringObject(sdsdup(key), sdslen(key));
        if (expireIfNeeded(c->db, keyobj) == 0) {
            sprintf(str_buf, "key: %s, val: %s", (sds) key, (sds) valobj->ptr);
            addReplyBulkCString(c, str_buf);
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
    char str_buf[1024];

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
        robj *valobj = dictGetVal(de);

        keyobj = createStringObject(sdsdup(key), sdslen(key));
        if (expireIfNeeded(c->db, keyobj) == 0) {
            sprintf(str_buf, "key: %s, val: %s", (sds) key, (sds) valobj->ptr);
            addReplyBulkCString(c, str_buf);
            numreplies++;
        }
        decrRefCount(keyobj);
    }

    dictReleaseIterator(di);
    setDeferredMultiBulkLength(c, replylen, numreplies);
}

void getListPmemStatusCommand(client *c) {
    void *replylen = addDeferredMultiBulkLength(c);
    unsigned long numreplies = 0;
    char str_buf[1024];
    TOID(struct redis_pmem_root) root;
    TOID(struct key_val_pair_PM) node_toid;
    struct key_val_pair_PM *node_obj;
    void *key;
    void *val;
    void *pmem_base_addr;

    root = server.pm_rootoid;
    pmem_base_addr = (void *)server.pm_pool->addr;
    node_toid = D_RO(root)->pe_first;
    if (TOID_IS_NULL(node_toid)) {
        setDeferredMultiBulkLength(c, replylen, numreplies);
        return;
    }
    while (1) {
        node_obj = (key_val_pair_PM *)(
                node_toid.oid.off + (uint64_t) pmem_base_addr
        );
        key = (void *)(node_obj->key_oid.off + (uint64_t) pmem_base_addr);
        val = (void *)(node_obj->val_oid.off + (uint64_t) pmem_base_addr);

        sprintf(str_buf, "key: %s, val: %s", (sds) key, (sds) val);
        addReplyBulkCString(c, str_buf);
        numreplies++;

        if (TOID_EQUALS(node_toid, D_RO(root)->pe_last))
            break;

        node_toid = D_RO(node_toid)->pmem_list_next;
    }

    setDeferredMultiBulkLength(c, replylen, numreplies);
}

void getReverseListPmemStatusCommand(client *c) {
    void *replylen = addDeferredMultiBulkLength(c);
    unsigned long numreplies = 0;
    char str_buf[1024];
    TOID(struct redis_pmem_root) root;
    TOID(struct key_val_pair_PM) node_toid;
    struct key_val_pair_PM *node_obj;
    void *key;
    void *val;
    void *pmem_base_addr;

    root = server.pm_rootoid;
    pmem_base_addr = (void *)server.pm_pool->addr;
    node_toid = D_RO(root)->pe_last;
    if (TOID_IS_NULL(node_toid)) {
        setDeferredMultiBulkLength(c, replylen, numreplies);
        return;
    }
    while (1) {
        node_obj = (key_val_pair_PM *)(
                node_toid.oid.off + (uint64_t) pmem_base_addr
        );
        key = (void *)(node_obj->key_oid.off + (uint64_t) pmem_base_addr);
        val = (void *)(node_obj->val_oid.off + (uint64_t) pmem_base_addr);

        sprintf(str_buf, "key: %s, val: %s", (sds) key, (sds) val);
        addReplyBulkCString(c, str_buf);
        numreplies++;

        if (TOID_EQUALS(node_toid, D_RO(root)->pe_first))
            break;

        node_toid = D_RO(node_toid)->pmem_list_prev;
    }

    setDeferredMultiBulkLength(c, replylen, numreplies);
}

void getListVictimStatusCommand(client *c) {
    void *replylen = addDeferredMultiBulkLength(c);
    unsigned long numreplies = 0;
    char str_buf[1024];
    TOID(struct redis_pmem_root) root;
    TOID(struct key_val_pair_PM) victim_toid;
    struct key_val_pair_PM *victim_obj;
    void *key;
    void *val;
    void *pmem_base_addr;

    root = server.pm_rootoid;
    pmem_base_addr = (void *)server.pm_pool->addr;
    for (victim_toid = D_RO(root)->victim_first;
        TOID_IS_NULL(victim_toid) == 0;
        victim_toid = D_RO(victim_toid)->pmem_list_next
    ) {
        victim_obj = (key_val_pair_PM *)(victim_toid.oid.off + (uint64_t) pmem_base_addr);
        key = (void *)(victim_obj->key_oid.off + (uint64_t) pmem_base_addr);
        val = (void *)(victim_obj->val_oid.off + (uint64_t) pmem_base_addr);

        sprintf(str_buf, "key: %s, val: %s", (sds) key, (sds) val);
        addReplyBulkCString(c, str_buf);
        numreplies++;
    }

    setDeferredMultiBulkLength(c, replylen, numreplies);
}
#endif
